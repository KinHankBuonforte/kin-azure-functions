const snowflake = require('snowflake-sdk');
const axios = require('axios').default;

const initSnowflakeConnection = () => {
    return new Promise((resolve, reject) => {
        var connection = snowflake.createConnection({
            account: 'ch10140.us-central1.gcp',
            username: 'HANKB',
            password: 'HankB123',
            application: 'COMPUTE_WH',
        });

        connection.connect((err, conn) => {
            if (err) {
                reject('Unable to connect: ' + err.message);
            } else {
                connection.execute({
                    sqlText: 'USE ROLE ACCOUNTADMIN',
                    complete: () => {
                        connection.execute({
                            sqlText: 'USE Database QB_PROJECTS',
                            complete: () => {
                                connection.execute({
                                    sqlText: 'USE schema PUBLIC',
                                    complete: () => {
                                        connection.execute({
                                            sqlText: 'USE WAREHOUSE COMPUTE_WH',
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
                    console.log(sql);
                    reject(
                        'Failed to execute statement due to the following error: ' +
                            err.message
                    );
                } else {
                    resolve(rows);
                }
            },
        });
    });
};

const formatLabel = (label) =>
    label
        .match(
            /[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+/g
        )
        .map((s) => s.toUpperCase())
        .join('_');

const fetchQuickbaseProjects = async function () {
    const { data } = await axios.post(
        'https://api.quickbase.com/v1/records/query',
        {
            from: 'br9kwm8na',
        },
        {
            headers: {
                'QB-Realm-Hostname': 'kin.quickbase.com',
                Authorization: `QB-USER-TOKEN b8gnqt_p3bs_0_di3bii2dt3jjyjcrvarkfbw53yrk`,
            },
        }
    );

    const records = data.data.map((item) => {
        const r = {};

        for (const key of Object.keys(item)) {
            const id = +key;
            const value = item[key].value;
            const field = data.fields.find((f) => f.id === id);
            r[formatLabel(field.label)] = value;
        }
        return r;
    });

    const columns = data.fields.map((f) => formatLabel(f.label));

    const connection = await initSnowflakeConnection();

    console.log('Creating table');
    // create table if not exists with columns
    await executeSql(
        connection,
        `CREATE OR REPLACE TABLE QUICKBASE_PROJECTS (${columns
            .map((c) => `${c} VARCHAR`)
            .join(', ')})`
    );

    console.log('Inserting rows');

    const sqlString = (value) => {
        if (!value) {
            return 'NULL';
        } else if (typeof value == 'string') {
            return `'${value.replace(/'/g, '"')}'`;
        } else if (typeof value == 'number') {
            return value.toString();
        } else if (typeof value == 'object') {
            return `'${JSON.stringify(value)}'`;
        } else {
            return value;
        }
    };

    const values = records
        .map(
            (record) =>
                `(${columns.map((x) => sqlString(record[x])).join(', ')})`
        )
        .join(', ');

    await executeSql(
        connection,
        `INSERT INTO QUICKBASE_PROJECTS (${columns.join(
            ', '
        )}) VALUES ${values}`
    );
};

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();

    if (myTimer.isPastDue) {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);

    await fetchQuickbaseProjects();
};
