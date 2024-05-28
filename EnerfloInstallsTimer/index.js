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
  const tableName = "ENERFLO_INSTALLS";
  const url =
    "https://enerflo.io/api/v3/installs?api_key=13686046e8dc420946.70185370";
  const pageSize = 50;
  const generateUrl = (page) => `${url}&page=${page}&per_page=${pageSize}`;

  const pages = 1;
  const records = [];
  const [columnsConfig, forceCreateTable] = await getColumnsConfig(
    connection,
    "ENERFLO",
    tableName
  );

  if (forceCreateTable) {
    const maxIdInfo = await executeSql(
      connection,
      `SELECT MAX(ID) FROM ${tableName}`
    );
    const maxId = maxIdInfo[0]["MAX(ID)"];
    console.log(`Max ID is ${maxId}`);

    let currentId = maxId + 1;
    let pageIndex = pages;

    while (currentId > maxId) {
      console.log(`Getting page ${pageIndex}`);
      data = (await axios.get(generateUrl(pageIndex))).data;
      const dataRecords = data.results;
      dataRecords.sort((a, b) => a.id - b.id);
      currentId = dataRecords[0].id;
      console.log(
        `dataRecords ids: ${dataRecords[0].id} - ${
          dataRecords[dataRecords.length - 1].id
        }`
      );
      console.log("New current id: " + currentId);
      records.push(...dataRecords.filter((x) => x.id > maxId));
      console.log(`Page ${pageIndex} parsed`);
      pageIndex += 1;
    }
  } else {
    for (let i = 1; i <= pages; i++) {
      console.log(`Getting page ${i}`);
      data = (await axios.get(generateUrl(i))).data;
      records.push(...data.results);
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
  await insertRecords(connection, flattenRecords, tableName, columns);

  context.res = {
    status: 200 /* Defaults to 200 */,
  };
};
