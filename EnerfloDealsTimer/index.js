const { gql, GraphQLClient } = require("graphql-request");
const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");

const client = new GraphQLClient("https://kinhome.enerflo.io/graphql", {
  headers: {
    authorization:
      "bearer c42ed5acf0114730fd4130d1a19f7e816ae7985249cd1f9d47d4958b55f1690c",
  },
});

const fetchDeals = async () => {
  let page = 1;
  const records = [];

  while (1) {
    const document = gql`
      query MyQuery($page: Float!) {
        fetchDealList(input: { pageSize: 300, page: $page }) {
          currentPage
          pageSize
          totalItemCount
          totalPageCount
          items {
            createdAt
            updatedAt
            currentStage
            currentSubStage
            dispositionNotes
            customer {
              firstName
              lastName
              id
              email
            }
            disposition {
              id
              label
              status
            }
            id
            progress
            shortCode
            state
            status
            templateVersionIsValid
            updatedAt
            writable
            org {
              id
              name
              subdomain
              status
            }
            installer {
              id
              name
              status
              isActive
            }
            utilityBills {
              id
              fileUpload {
                bucket
                contentType
                isPublic
                createdAt
                key
                region
                size
                updatedAt
                uploadedAt
                signedUrl
                originalFilename
                id
              }
            }
          }
        }
      }
    `;
    const { fetchDealList: res } = await client.request(document, { page });
    records.push(...res.items);

    if (res.currentPage === res.totalPageCount) {
      break;
    }
    page += 1;
  }

  const connection = await initSnowflakeConnection();
  const tableName = "ENERFLO_DEALS";
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

  if (obj.utilityBills && obj.utilityBills.length) {
    obj.utilityBills = obj.utilityBills[0];

    for (const key of Object.keys(obj.utilityBills.fileUpload)) {
      obj.utilityBills[`fileUpload_${key}`] = obj.utilityBills.fileUpload[key];
    }
    delete obj.utilityBills.fileUpload;
  } else {
    delete obj.utilityBills;
  }

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
      const column = columns.find((c) => c.name === key);

      if (column) {
        column.values.push(value);
      } else {
        columns.push({
          name: key,
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

  await executeSql(
    connection,
    `CREATE OR REPLACE TABLE ${tableName} (${columns
      .map((x) => `${snakecase(x.name).toUpperCase()} ${x.type}`)
      .join(", ")})`
  );
  return columns.map((x) => ({
    field: x.name,
    column: snakecase(x.name).toUpperCase(),
    type: x.type,
  }));
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
    return `'${(`${value || ""}`).replace(/'/g, '"')}'`;
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
    await fetchDeals();

    context.res = {
      status: 200 /* Defaults to 200 */,
    };
  } catch (err) {
    console.error(err);
    throw err;
  }
};
