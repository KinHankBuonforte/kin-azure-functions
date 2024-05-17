const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");
const axios = require("axios");

const api = axios.default.create({
  baseURL: "https://enzyhr.com",
  headers: {
    Authorization: "Bearer KIN_DEV_O2h4d88TkQcO4Mm",
  },
});

const fetchLeads = async function () {
  const { data } = await api.get("/rest/externalApi/leads");

  if (!data.success || !data.output) {
    context.log("No data");
    return;
  }
  const leads = data.output;
  const connection = await initSnowflakeConnection();
  const flattenRecords = leads.map((x) => flattenObject(x));

  const appointments = [];

  for (const r of flattenRecords) {
    const leadAppointments = (r.appointments ?? []).map((ap) => ({
      ...ap,
      leadId: r.leadId,
    }));
    appointments.push(...leadAppointments);
    delete r.appointments;
  }
  await insertRecords(connection, flattenRecords, "ENZY_LEADS");

  const flattenedAppointments = appointments.map((x) => flattenObject(x));
  await insertRecords(connection, flattenedAppointments, "ENZY_APPOINTMENTS");
};

module.exports = async function (context, myTimer) {
  context.log("Start fetching leads...", timeStamp);
  await fetchLeads();
};

const insertRecords = async (connection, records, tableName) => {
  const columns = await initTable(connection, tableName, records);

  for (let i = 0; i < records.length; i += 100) {
    console.log(`Inserting ${i} batch from ${records.length}`);
    const values = records
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
              sqlText: "USE Database ENZY_KIN",
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
