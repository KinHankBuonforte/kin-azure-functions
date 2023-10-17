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

const formatLabel = (label) => {
    const newLabel = label
        .match(
            /[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+/g
        )
        .map((s) => s.toUpperCase())
        .join('_');

    return newLabel.match(/^\d/) || newLabel === 'CURRENT_USER'
        ? `_${newLabel}`
        : newLabel;
};

const fetchQuickbaseProjects = async function () {
    const tableId = 'br9kwm8na';
    const hostName = 'kin.quickbase.com';
    const authToken = `QB-USER-TOKEN b8gnqt_p3bs_0_di3bii2dt3jjyjcrvarkfbw53yrk`;

    const headers = {
        'QB-Realm-Hostname': hostName,
        Authorization: authToken,
    };

    const { data: fields } = await axios.get(
        'https://api.quickbase.com/v1/fields',
        {
            params: {
                tableId,
            },
            headers,
        }
    );

    const { data } = await axios.post(
        'https://api.quickbase.com/v1/records/query',
        {
            from: tableId,
        },
        {
            headers,
        }
    );
    const newFields = fields.map((f) => ({
        id: f.id,
        uniqueLabel: formatLabel(f.label),
    }));

    for (const field of newFields) {
        const sameFields = newFields.filter(
            (f) => f.uniqueLabel === field.uniqueLabel
        );

        if (sameFields.length === 1) {
            continue;
        }
        field.index = sameFields.findIndex((f) => f.id === field.id) + 1;
    }

    for (const field of newFields) {
        if (!field.index) {
            continue;
        }
        field.uniqueLabel = field.uniqueLabel + '__' + field.index;
    }

    const records = data.data.map((item) => {
        const r = {};

        for (const key of Object.keys(item)) {
            const id = +key;
            const value = item[key].value;
            const field = newFields.find((f) => f.id === id);
            r[field.uniqueLabel] = value;
        }
        return r;
    });

    const columns = newFields.map((f) => f.uniqueLabel);

    const connection = await initSnowflakeConnection();

    console.log('Creating table');

    try {
        // create table if not exists with columns
        await executeSql(
            connection,
            `CREATE OR REPLACE TABLE QUICKBASE_PROJECTS (${columns
                .map((c) => `${c} VARCHAR`)
                .join(', ')})`
        );
    } catch (err) {
        console.error('Failed to create table');
        console.error(err);
        return;
    }

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

    try {
        await executeSql(
            connection,
            `INSERT INTO QUICKBASE_PROJECTS (${columns.join(
                ', '
            )}) VALUES ${values}`
        );
    } catch (err) {
        console.error('Failed to insert records');
        console.error(err);
    }
};

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();

    if (myTimer.isPastDue) {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);

    await fetchQuickbaseProjects();
};
