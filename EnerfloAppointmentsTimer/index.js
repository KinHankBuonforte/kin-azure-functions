const {
  initSnowflakeConnection,
  flattenObject,
  initTable,
  insertRecords,
  executeSql,
  getColumnsConfig,
} = require("../snowflake");
const axios = require("axios").default;

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    console.log("JavaScript is running late!");
  }
  console.log("JavaScript timer trigger function ran!", timeStamp);

  // get records
  const connection = await initSnowflakeConnection();
  const tableName = "ENERFLO_APPOINTMENTS";
  const [columnsConfig, forceCreateTable] = await getColumnsConfig(
    connection,
    "ENERFLO",
    tableName
  );

  const url =
    "https://enerflo.io/api/v1/appointments?api_key=13686046e8dc420946.70185370";
  const pageSize = 1000;
  const generateUrl = (page) => `${url}&page=${page}&pageSize=${pageSize}`;
  let data = (await axios.get(url)).data;

  const dataCount = data.dataCount;
  const pages = Math.ceil(dataCount / pageSize);
  const records = [];

  if (!forceCreateTable) {
    const maxIdInfo = await executeSql(
      connection,
      `SELECT MAX(ID) FROM ${tableName}`
    );
    const maxId = maxIdInfo[0]["MAX(ID)"];
    console.log("Max id: " + maxId);
    let currentId = maxId + 1;
    let pageIndex = pages;
    while (currentId > maxId) {
      console.log(`Getting page ${pageIndex}`);
      data = (await axios.get(generateUrl(pageIndex))).data;
      const dataRecords = data.data;
      if (dataRecords.length == 0) {
        break;
      }
      dataRecords.sort((a, b) => b.id - a.id);
      currentId = dataRecords[dataRecords.length - 1].id;
      console.log(
        `dataRecords ids: ${dataRecords[0].id} - ${
          dataRecords[dataRecords.length - 1].id
        }`
      );
      console.log("New current id: " + currentId);
      records.push(...dataRecords.filter((x) => x.id > maxId));
      console.log(`Page ${pageIndex} parsed`);
      pageIndex -= 1;
    }
  } else {
    for (let i = 1; i <= pages; i++) {
      console.log(`Getting page ${i}`);
      data = (await axios.get(generateUrl(i))).data;
      records.push(...data.data);
      console.log(`Page ${i} parsed`);
    }
  }
  console.log(`Records found: ${records.length}`);
  // convert records to flat records
  const flattenRecords = records.map((x) => flattenObject(x));
  if (flattenRecords.length == 0) {
    return;
  }
  const columns = await initTable(
    connection,
    "ENERFLO",
    tableName,
    flattenRecords,
    columnsConfig,
    forceCreateTable
  );
  await insertRecords(connection, records, tableName, columns);

  context.res = {
    status: 200 /* Defaults to 200 */,
  };
};
