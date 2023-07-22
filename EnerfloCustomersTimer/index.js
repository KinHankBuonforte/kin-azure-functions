const snowflake = require("snowflake-sdk");
const axios = require("axios").default;

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.isPastDue)
    {
        console.log('JavaScript is running late!');
    }
    console.log('JavaScript timer trigger function ran!', timeStamp);   
    const initSnowflakeConnection = () => {
        return new Promise((resolve, reject) => {
            var connection = snowflake.createConnection({
                account: 'ch10140.us-central1.gcp',
                username: 'HANKB',
                password: 'HankB123',
                application: 'COMPUTE_WH'
            });
    
            connection.connect((err, conn) => {
                if (err) {
                    reject('Unable to connect: ' + err.message);
                } else {
                    connection.execute({
                        sqlText: "USE ROLE ACCOUNTADMIN", complete: () => {
                            connection.execute({
                                sqlText: "USE Database ENERFLO", complete: () => {
                                    connection.execute({
                                        sqlText: "USE schema PUBLIC", complete: () => {
                                            connection.execute({
                                                sqlText: "USE WAREHOUSE COMPUTE_WH", complete: () => {
                                                    resolve(connection); 
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        });
    }

    const executeSql = (connection, sql) => {
        return new Promise((resolve, reject) => {
            connection.execute({
                sqlText: sql, complete: (err, stmt, rows) => {
                    if (err) {
                        reject('Failed to execute statement due to the following error: ' + err.message);
                    } else {
                        resolve(rows);
                    }
                }
            })
        });
    }

    const sqlString = (value) => {
        if(typeof value == "string") {
            return `'${value.replace(/'/g, '"')}'`;
        } else if (typeof value == "number") {
            return value.toString();
        } else if (!value) {
            return "NULL";
        } else {
            return value.toString();
        }
    }

    const flattenObject = (obj) => {
        const flattened = {};

        for(let key of Object.keys(obj)) {
          const value = obj[key];
      
          if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
            for(let valueKey of Object.keys(value)) {
                const valueValue = value[valueKey];            
                if ((typeof valueValue === 'object' || Array.isArray(valueValue)) && valueValue !== null) {
                  flattened[key + '_' + valueKey] = JSON.stringify(valueValue);
                }
                else {
                  flattened[key + '_' + valueKey] = valueValue;
                }
              }
          }
          else if(Array.isArray(value)) {
            flattened[key] = JSON.stringify(value);
          }
          else {
            flattened[key] = value;
          }
        }
      
        return flattened;
    }

    const initTable = async (connection, tableName, records) => {
        let columns = [];
        for(let record of records) {
            for(let key of Object.keys(record)) {
                const value = record[key];
                if(/\s|-/.test(key)) {
                    continue;
                }
                if(value) {
                    let column = columns.find(c => c.name == key);
                    if(!column) {
                        column = { name: key, values: [value]};
                        columns.push(column);
                    } else {
                        column.values.push(value);
                    }    
                }
            }
        }
        for(let column of columns) {
            if(typeof column.values[0] == "boolean") {
                column.type = "BOOLEAN";
            } else if(typeof column.values[0] == "number") {
                if(column.values.some(x => x.toString().indexOf('.') != -1)) {
                    column.type = "FLOAT";
                } else {
                    column.type = "INT";
                }
            } else {
                column.type = "VARCHAR";
            }
        }
        // get columns that exists
        const existingColumns = await executeSql(connection, `SELECT COLUMN_NAME FROM ENERFLO.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='${tableName}'`);
        if(existingColumns.length > 0) {
            // for each new column add column
            for(let column of columns) {
                if(!existingColumns.some(x => x['COLUMN_NAME'] == column.name.toUpperCase())) {
                    await executeSql(connection, `ALTER TABLE ${tableName} ADD ${column.name} ${column.type}`);
                }
            }
        }
        else {
            // create table if not exists with columns
            await executeSql(connection, `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.map(x => `${x.name.toUpperCase()} ${x.type}`).join(", ")})`)
        }
        return columns.map(x => x.name.toUpperCase());
    }

    const checkTableExists = async (connection, tableName) => {
        const existingColumns = await executeSql(connection, `SELECT COLUMN_NAME FROM ENERFLO.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='${tableName}'`);
        return existingColumns.length > 0;
    }
    // get records
    const connection = await initSnowflakeConnection();

    const tableName = 'ENERFLO_CUSTOMERS';
    const url = 'https://enerflo.io/api/v1/customers?api_key=13686046e8dc420946.70185370';
    const pageSize = 1000;
    const generateUrl = (page) => `${url}&page=${page}&pageSize=${pageSize}`;
    let data = (await axios.get(url)).data;

    const dataCount = data.dataCount;
    const pages = Math.ceil(dataCount / pageSize);
    const records = [];

    const tableExists = await checkTableExists(connection, tableName);
    if(tableExists) {
        const maxIdInfo = await executeSql(connection, `SELECT MAX(ID) FROM ${tableName}`);
        const maxId =maxIdInfo[0]['MAX(ID)'];
        console.log("Max id: " + maxId);
        let currentId = maxId + 1;
        let pageIndex = pages;
        while(currentId > maxId) {
            console.log(`Getting page ${pageIndex}`);
            data = (await axios.get(generateUrl(pageIndex))).data;
            const dataRecords = data.data;
            if(dataRecords.length == 0) {
                break;
            }
            dataRecords.sort((a, b) => b.id - a.id);
            currentId = dataRecords[dataRecords.length-1].id;
            console.log(`dataRecords ids: ${dataRecords[0].id} - ${dataRecords[dataRecords.length-1].id}`);
            console.log('New current id: ' + currentId);
            records.push(...dataRecords.filter(x => x.id > maxId));
            console.log(`Page ${pageIndex} parsed`);
            pageIndex -= 1;
        }
    } else {
        for(let i = 1; i <= pages; i++) {
            console.log(`Getting page ${i}`);
            data = (await axios.get(generateUrl(i))).data;
            records.push(...data.data);
            console.log(`Page ${i} parsed`);
        }    
    }
    console.log(`Records found: ${records.length}`);
    // convert records to flat records
    const flattenRecords = records.map(x => flattenObject(x));   
    if(flattenRecords.length == 0) {
        return;
    }
    const columns = await initTable(connection, tableName, flattenRecords);

    for(let i = 0; i < flattenRecords.length; i+= 100) {
        console.log(`Inserting ${i} batch from ${flattenRecords.length}`);
        const values = flattenRecords.slice(i, i+100).map(record => `(${columns.map(x => sqlString(record[x.toLowerCase()])).join(", ")})`).join(', ');
        await executeSql(connection, `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES ${values}`);
    }
    context.res = {
        status: 200, /* Defaults to 200 */
    };  
};

//   module.exports({}, {});