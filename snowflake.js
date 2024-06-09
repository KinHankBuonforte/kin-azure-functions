require("dotenv").config();
const snowflake = require("snowflake-sdk");
const snakecase = require("snakecase");
const { fetchColumns } = require("./fetch-columns");

class Snowflake {
  connection = null;
  context = console;
  dbName = "";

  constructor(context) {
    this.context = context ?? console;
  }

  static async create(context = console, dbName = "ENERFLO") {
    const instance = new Snowflake(context ?? console);
    await instance.connect(dbName);

    return instance;
  }

  async connect(dbName = "ENERFLO") {
    this.connection = await new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: "ch10140.us-central1.gcp",
        username: "HANKB",
        password: "HankB123",
        application: "COMPUTE_WH",
      });
      const database = getDBName(dbName);

      connection.connect((err, conn) => {
        if (err) {
          reject("Unable to connect: " + err.message);
        } else {
          connection.execute({
            sqlText: "USE ROLE ACCOUNTADMIN",
            complete: () => {
              connection.execute({
                sqlText: `USE Database ${database}`,
                complete: () => {
                  connection.execute({
                    sqlText: "USE schema PUBLIC",
                    complete: () => {
                      connection.execute({
                        sqlText: "USE WAREHOUSE COMPUTE_WH",
                        complete: () => {
                          this.dbName = database;
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
    this.context.log("Connected to:", this.dbName);
    return this.connection;
  }

  async execute(sql) {
    if (!this.connection) {
      throw new Error("No DB connection");
    }
    return new Promise((resolve, reject) => {
      this.connection.execute({
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
  }

  async insert(records, tableName, columns, newTable = true) {
    for (let i = 0; i < records.length; i += 100) {
      this.context.log(
        `${tableName}: Inserting ${i} - ${
          i + 100 >= records.length ? records.length : i + 100
        } from ${records.length}`
      );
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
        await this.execute(
          `INSERT INTO ${tableName}${newTable ? "_TEMP" : ""} (${columns
            .map((c) => c.column)
            .join(", ")}) VALUES ${values}`
        );
      } catch (err) {
        this.context.error(err);
        break;
      }
    }

    if (newTable) {
      try {
        this.context.log(`${tableName}: drop`);
        await this.execute(`DROP TABLE ${tableName}`);
      } catch (err) {
        this.context.error(`${tableName} drop failed:`, err);
      }

      try {
        this.context.log(`${tableName}_TEMP: moving to current`);
        await this.execute(
          `ALTER TABLE ${tableName}_TEMP RENAME TO ${tableName}`
        );
      } catch (err) {
        this.context.error(`${tableName}_TEMPS rename failed:`, err);
      }
    }
  }

  async createOrUpdateTable(
    tableName,
    records,
    columnsConfig,
    forceCreateTable = false,
    idField = "id"
  ) {
    this.context.log("Table Configuration:", this.dbName, tableName);
    this.context.log("Table Needs recreate:", forceCreateTable);

    const columns = [];

    for (const record of records) {
      for (let key of Object.keys(record)) {
        const value = record[key];
        const column = columns.find((c) => c.field === key);
        let columnName = snakecase(key).toUpperCase();

        if (columnsConfig) {
          const config = columnsConfig.find(
            (c) => c.originalColumnName.toUpperCase() === columnName
          );

          if (!config) {
            continue;
          }
          columnName = config.newName.toUpperCase();
        }

        if (column) {
          column.values.push(value);
        } else {
          columns.push({
            field: key,
            column: columnName,
            values: [value],
          });
        }
      }
    }

    for (const column of columns) {
      const values = column.values.filter((v) => v !== null && v !== undefined);
      const types = values.map((v) => typeof v);

      if (!values.length) {
        records.forEach((r) => (r[column.field] = null));
        column.type = "VARCHAR";
      } else if (Array.from(new Set(types)).length > 1) {
        column.type = "VARCHAR";
      } else {
        if (typeof values[0] === "boolean") {
          column.type = "BOOLEAN";
        } else if (typeof values[0] === "number") {
          const isFloat = values.some((v) => `${v}`.includes("."));
          column.type = isFloat ? "FLOAT" : "INT";
        } else {
          column.type = "VARCHAR";
        }
      }
    }
    columns.push({
      type: "VARCHAR",
      column: "INSERTED_AT",
      field: "inserted_at",
    });

    for (const record of records) {
      record.inserted_at = new Date().toUTCString();
    }

    if (!forceCreateTable) {
      const existingColumns = await this.getExistingColumns(tableName);

      for (let column of columns) {
        if (!existingColumns.some((x) => x === column.column)) {
          await this.execute(
            `ALTER TABLE ${tableName} ADD ${column.column} ${column.type}`
          );
        }
      }
      await this.execute(
        `DELETE FROM ${tableName} WHERE ${idField.toUpperCase()} IN (${records.map(
          (r) => `'${r[idField]}'`
        )})`
      );
    } else {
      await this.execute(
        `CREATE OR REPLACE TABLE ${tableName}_TEMP (${columns
          .map((x) => `${x.column} ${x.type}`)
          .join(", ")})`
      );
    }
    return columns.map(({ values, ...column }) => column);
  }

  async getExistingColumns(tableName) {
    try {
      const existingColumns = await this.execute(
        `SHOW COLUMNS IN TABLE ${this.dbName}.PUBLIC.${tableName}`
      );
      return existingColumns.map((c) => c.column_name);
    } catch (err) {
      return [];
    }
  }

  async checkTableExists(tableName) {
    const existingColumns = await this.getExistingColumns(tableName);
    return existingColumns.length > 0;
  }

  async getColumnsConfig(tableId, requiredColumns = ["ID", "INSERTED_AT"]) {
    if (process.env.FRESH_RUN) {
      return [null, true];
    }
    try {
      const {
        data: { config, columns },
      } = await fetchColumns(this.dbName, tableId);
      const existingColumns = await this.getExistingColumns(tableId);
      const columnsConfig = [...requiredColumns, ...columns]
        .map((c) => ({
          originalColumnName: c,
          newName: config[c]?.name ?? c,
          excluded: config[c]?.excluded ?? false,
        }))
        .filter((c) => !c.excluded);

      const newColumnNames = columnsConfig
        .map((c) => c.newName)
        .sort((a, b) => (a > b ? 1 : -1));

      console.log(
        newColumnNames.length,
        existingColumns.sort((a, b) => (a > b ? 1 : -1)).length
      );

      const forceUpdate =
        existingColumns.sort((a, b) => (a > b ? 1 : -1)).join(",") !==
        newColumnNames.join(",");
      return [columnsConfig, forceUpdate];
    } catch (err) {
      throw err?.response?.data ?? err?.response ?? err;
    }
  }
}

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

const sqlString = (value, type) => {
  if (value === null || value === undefined) {
    return "NULL";
  }
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

const getDBName = (dbName) =>
  (
    dbName + (process.env.NODE_ENV ? `_${process.env.NODE_ENV}` : "")
  ).toUpperCase();

module.exports = {
  sqlString,
  flattenObject,
  getDBName,
  Snowflake,
};
