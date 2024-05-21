const { gql, GraphQLClient } = require("graphql-request");
const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");

const client = new GraphQLClient("https://kinhome.enerflo.io/graphql", {
  headers: {
    authorization:
      "bearer c42ed5acf0114730fd4130d1a19f7e816ae7985249cd1f9d47d4958b55f1690c",
  },
});

const fetchCustomers = async () => {
  let page = 1;
  const records = [];

  const connection = await initSnowflakeConnection();
  const tableName = "ENERFLO_CUSTOMERS";

  let lastDate;

  try {
    const qLastDate = await executeSql(
      connection,
      `SELECT MAX(UPDATED_AT) FROM ${tableName}`
    );
    lastDate = qLastDate[0]?.["MAX(UPDATED_AT)"];
  } catch (err) {}

  console.log(lastDate);

  while (1) {
    const document = gql`
      query MyQuery($page: Float!, $date: String!) {
        fetchCustomerList(
          input: {
            pageSize: 300
            page: $page
            filter: { updatedAt: { _gt: $date } }
            orderBy: { by: ASC, field: "updatedAt" }
          }
        ) {
          currentPage
          pageSize
          totalItemCount
          totalPageCount
          items {
            id
            firstName
            lastName
            mobile
            language
            email
            createdAt
            address {
              city
              country
              lat
              line1
              line2
              line3
              lng
              postalCode
              state
            }
            phone
            phoneIsMobile
            status
            tz
            updatedAt
          }
        }
      }
    `;
    const { fetchCustomerList: res } = await client.request(document, {
      page,
      date: lastDate ?? "2000-01-01",
    });
    records.push(...res.items);

    if (res.currentPage === res.totalPageCount) {
      break;
    }
    page += 1;
  }
  if (!records.length) {
    console.log("No records to insert");
    return;
  }
  console.log(`${records.length} records to insert`)
  const flattenRecords = records.map((x) => flattenObject(x));
  const columns = await initTable(connection, tableName, flattenRecords);

  for (let i = 0; i < flattenRecords.length; i += 100) {
    console.log(`Inserting ${i} batch from ${flattenRecords.length}`);
    const values = flattenRecords
      .slice(i, i + 100)
      .map(
        (record) =>
          `(${columns
            .map((x) => sqlString(record[x.field], x.type))
            .join(", ")})`
      )
      .join(", ");

    try {
      await executeSql(
        connection,
        `INSERT INTO ${tableName} (${columns
          .map((c) => c.column)
          .join(", ")}) VALUES ${values}`
      );
    } catch (err) {
      console.error(err);
      break;
    }
  }
};

const flattenObject = (obj) => {
  const flattened = {};

  for (let key of Object.keys(obj)) {
    const value = obj[key];

    if (typeof value === "object" && !Array.isArray(value) && value !== null) {
      for (let valueKey of Object.keys(value)) {
        const valueValue = value[valueKey];
        if (
          (typeof valueValue === "object" || Array.isArray(valueValue)) &&
          valueValue !== null
        ) {
          flattened[key + "_" + valueKey] = JSON.stringify(valueValue);
        } else {
          flattened[key + "_" + valueKey] = valueValue;
        }
      }
    } else if (Array.isArray(value)) {
      flattened[key] = JSON.stringify(value);
    } else {
      flattened[key] = value;
    }
  }

  return flattened;
};

const initTable = async (connection, tableName, records) => {
  let columns = [];
  for (const record of records) {
    for (let key of Object.keys(record)) {
      const value = record[key];
      const column = columns.find((c) => c.field === key);

      if (column) {
        column.values.push(value);
      } else {
        columns.push({
          field: key,
          column: snakecase(key).toUpperCase(),
          values: [value],
        });
      }
    }
  }

  for (const column of columns) {
    const values = column.values.filter((v) => v !== null && v !== undefined);
    const types = values.map((v) => typeof v);

    if (!values.length || Array.from(new Set(types)).length > 1) {
      column.type = "VARCHAR";
    } else {
      if (typeof values[0] == "boolean") {
        column.type = "BOOLEAN";
      } else if (typeof values[0] == "number") {
        column.type = "FLOAT";
      } else {
        column.type = "VARCHAR";
      }
    }
  }

  let existingColumns = [];

  try {
    existingColumns = await executeSql(
      connection,
      `SHOW COLUMNS IN TABLE ENERFLO.PUBLIC.${tableName}`
    );
  } catch (err) {}

  if (existingColumns.length) {
    for (let column of columns) {
      if (!existingColumns.some((x) => x.column_name === column.column)) {
        await executeSql(
          connection,
          `ALTER TABLE ${tableName} ADD ${column.column} ${column.type}`
        );
      }
    }
    await executeSql(
      connection,
      `DELETE FROM ${tableName} WHERE ID IN (${records.map((r) => `'${r.id}'`)})`
    );
  } else {
    await executeSql(
      connection,
      `CREATE OR REPLACE TABLE ${tableName} (${columns
        .map((x) => `${x.column} ${x.type}`)
        .join(", ")})`
    );
  }
  return columns.map(({ values, ...column }) => column);
};

const initSnowflakeConnection = () => {
  return new Promise((resolve, reject) => {
    var connection = snowflake.createConnection({
      account: "ch10140.us-central1.gcp",
      username: "HANKB",
      password: "HankB123",
      application: "COMPUTE_WH",
    });

    connection.connect((err, conn) => {
      if (err) {
        reject("Unable to connect: " + err.message);
      } else {
        connection.execute({
          sqlText: "USE ROLE ACCOUNTADMIN",
          complete: () => {
            connection.execute({
              sqlText: "USE Database ENERFLO",
              complete: () => {
                connection.execute({
                  sqlText: "USE schema PUBLIC",
                  complete: () => {
                    connection.execute({
                      sqlText: "USE WAREHOUSE COMPUTE_WH",
                      complete: () => {
                        resolve(connection);
                      },
                    });
                  },
                });
              },
            });
          },
        });
      }
    });
  });
};

const executeSql = (connection, sql) => {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(
            "Failed to execute statement due to the following error: " +
              err.message
          );
        } else {
          resolve(rows);
        }
      },
    });
  });
};

const sqlString = (value, type) => {
  if (type === "VARCHAR") {
    return `'${`${value || ""}`.replace(/'/g, '"')}'`;
  } else if (type == "FLOAT") {
    return (value ?? "NULL").toString();
  } else if (type === "BOOLEAN") {
    return !!value;
  } else {
    return (value ?? "NULL").toString();
  }
};

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  try {
    await fetchCustomers();

    context.res = {
      status: 200 /* Defaults to 200 */,
    };
  } catch (err) {
    console.error(err);
    throw err;
  }
};
