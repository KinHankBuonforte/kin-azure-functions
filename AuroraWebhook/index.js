const snowflake = require('snowflake-sdk');
const axios = require('axios');

module.exports = async function (context, req) {

    const tenant_id = '034b1c47-310a-460f-9d5d-b625dd354f12';
    const bearer = 'sk_prod_5e9cc118a6e24ed265607bc4';

    const client = axios.create({
        baseURL: `https://api.aurorasolar.com/tenants/${tenant_id}/`,
        headers: {'authorization': `Bearer ${bearer}`, 'accept': 'application/json'}
    });

        
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

    context.log('Aurora request:');
    context.log(req.query);

    const con = await initSnowflakeConnection();
 
    const getValue = (value) => {
        if(typeof value == "number") {
            return value.toString();
        } else if(Array.isArray(value)) {
            return `PARSE_JSON('${JSON.stringify(value)}')`;
        } else {
            return `'${value}'`;   
        }
    }
    
    let projectResponse = await client.get(`projects/${req.query.id}`);
    let request = projectResponse.data.project;
    const columns = [];
    const values = [];
    for(let key of Object.keys(request).filter(k => !!request[k])) {
        columns.push(key.toUpperCase());
        values.push(`${getValue(request[key])} as ${key.toUpperCase()}`);
    }
    await executeSql(con, `INSERT INTO AURORA_PROJECTS (${columns.join(", ")}) SELECT ${values.join(", ")}`);

    context.res = {
        status: 200, /* Defaults to 200 */
    };  
}