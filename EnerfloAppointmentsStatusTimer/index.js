const axios = require('axios').default;
const snowflake = require("snowflake-sdk");

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

    const connection = await initSnowflakeConnection();
    const appointmentIds = await executeSql(connection, `SELECT ID from ENERFLO_APPOINTMENTS WHERE APPOINTMENT_STATUS IS NULL ORDER BY ID desc`);
    console.log(appointmentIds.length);
    let i = 0;
    for(let appointmentId of appointmentIds) {
        console.log(`${new Date()}. Running ${appointmentId['ID']}. ${i} out o  ${appointmentIds.length}`);
        let data = await axios(`https://enerflo.io/api/v1/appointments/${appointmentId['ID']}?api_key=13686046e8dc420946.70185370`, { method: 'PUT'});
        let appointmentStatus = "Not found";
        if (data.status != 404) {
            const jsonData = data.data;
            if(jsonData["appointment_status"]) {
                appointmentStatus = jsonData["appointment_status"];
            }
        }
        await executeSql(connection, `UPDATE ENERFLO_APPOINTMENTS SET APPOINTMENT_STATUS=\'${appointmentStatus}\' where ID=${appointmentId['ID']}`)
        i = i + 1;
    }

    context.res = {
        status: 200, /* Defaults to 200 */
    };  
};

// module.exports({}, {});