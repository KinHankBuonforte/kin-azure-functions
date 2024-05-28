const { initSnowflakeConnection, executeSql } = require("../snowflake");

module.exports = async function (context, req) {
    context.log('Enerflo request query:.');
    context.log(JSON.stringify(req.query));

    context.log('Enerflo request body:.');
    context.log(JSON.stringify(req.body));
    const request = req.body;

    // let request = {
    //     "webhook_event":"new_design_request",
    //     "id":754580,
    //     "status":"pending",
    //     "priority":"Standard",
    //     "is_revision":false,
    //     "sales_company_id":1368,
    //     "install_company_id":1368,
    //     "deal_id":1481414,
    //     "deal_file_id":null,
    //     "install_id":null,
    //     "assignee":{},
    //     "timestamps":{
    //         "viewed_at":null,
    //         "assigned_at":null,
    //         "created_at":"2023-04-29 01:36:19",
    //         "updated_at":"2023-04-29 01:36:19",
    //         "timezone":"UTC"
    //     }
    // };
    
    const flatRequest = (r) => {
        for(let key of Object.keys(r)) {
            if(r[key] && typeof r[key] === 'object') {
                for(let oKey of Object.keys(r[key])) {
                    r[key + '_' + oKey] = r[key][oKey];
                }
                delete r[key];
            }
        }
    }
    
    flatRequest(request);

    const con = await initSnowflakeConnection();

    const getType = (request, key) => {
        if(typeof request[key] == "number") {
            return 'NUMERIC'
        } else {
            return 'VARCHAR'
        }
    }
    const tableMetadata = Object.keys(request).map(k => `${k.toUpperCase()} ${getType(request, k)}`).join(", ");        
    context.log(tableMetadata);
    const columns = Object.keys(request).filter(k => !!request[k]).map(k => k.toUpperCase());
    context.log(columns);
    const values = Object.keys(request).filter(k => !!request[k]).map(k => typeof request[k] === "number" ? request[k].toString() : `'${request[k]}'`).join(", ");
    context.log(values);
    await executeSql(con, `CREATE TABLE IF NOT EXISTS ENERFLO_WEBHOOK_EVENTS (${tableMetadata})`);
    const existingColumns = (await executeSql(con, `SELECT COLUMN_NAME FROM ENERFLO.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ENERFLO_WEBHOOK_EVENTS'`)).map(x => x['COLUMN_NAME'].toUpperCase());
    const columnsToAdd = columns.filter(c => !existingColumns.some(x => x == c));
    for(let column of columnsToAdd) {
        await executeSql(con, `ALTER TABLE ENERFLO_WEBHOOK_EVENTS ADD COLUMN ${column} ${getType(request, Object.keys(request).find(x => x.toUpperCase() == column))}`);
    }
    await executeSql(con, `INSERT INTO ENERFLO_WEBHOOK_EVENTS (${columns.join(", ")}) VALUES (${values})`);
    context.res = {
        status: 200, /* Defaults to 200 */
    };  
}