const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");

const initSnowflakeConnection = (dbName = "ENERFLO") => {
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
              sqlText: `USE Database ${dbName}`,
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

const insertRecords = async (connection, records, tableName, columns) => {
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
    } else if (Array.isArray(value)) {
      flattened[key] = JSON.stringify(value);
    } else {
      flattened[key] = value;
    }
  }

  return flattened;
};

const initTable = async (
  connection,
  dbName,
  tableName,
  records,
  forceCreateTable = false,
  idField = "id"
) => {
  console.log("Table Configuration: ", dbName, tableName);

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
        const isFloat = values.some((v) => `${v}`.includes("."));
        column.type = isFloat ? "FLOAT" : "INT";
      } else {
        column.type = "VARCHAR";
      }
    }
  }

  const tableExists = await checkTableExists(connection, dbName, tableName);

  if (tableExists && !forceCreateTable) {
    let existingColumns = [];

    try {
      existingColumns = await executeSql(
        connection,
        `SHOW COLUMNS IN TABLE ${dbName}.PUBLIC.${tableName}`
      );
    } catch (err) {}

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
      `DELETE FROM ${tableName} WHERE ${idField.toUpperCase()} IN (${records.map(
        (r) => `'${r[idField]}'`
      )})`
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

const sqlString = (value, type) => {
  if (type === "VARCHAR") {
    return `'${`${value || ""}`.replace(/'/g, '"')}'`;
  } else if (type == "FLOAT" || type == "INT") {
    return (value ?? "NULL").toString();
  } else if (type === "BOOLEAN") {
    return !!value;
  } else {
    return (value ?? "NULL").toString();
  }
};

const checkTableExists = async (connection, dbName, tableName) => {
  const existingColumns = await executeSql(
    connection,
    `SELECT COLUMN_NAME FROM ${dbName}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='${tableName}'`
  );
  return existingColumns.length > 0;
};

module.exports = {
  executeSql,
  initSnowflakeConnection,
  insertRecords,
  initTable,
  sqlString,
  flattenObject,
  checkTableExists,
};
