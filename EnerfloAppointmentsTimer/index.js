const { Snowflake, flattenObject, getDBName } = require("../snowflake");
const axios = require("axios").default;

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  // get records
  const snowflake = await Snowflake.create(context, "ENERFLO");
  const tableName = "ENERFLO_APPOINTMENTS";
  const [columnsConfig, forceCreateTable] = await snowflake.getColumnsConfig(
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

  console.log(forceCreateTable);

  if (!forceCreateTable) {
    const maxIdInfo = await snowflake.execute(
      `SELECT MAX(ID) FROM ${tableName}`
    );
    const maxId = maxIdInfo[0]["MAX(ID)"];
    context.log("Max id: " + maxId);
    let currentId = maxId + 1;
    let pageIndex = pages;
    while (currentId > maxId) {
      context.log(`Getting page ${pageIndex}`);
      data = (await axios.get(generateUrl(pageIndex))).data;
      const dataRecords = data.data;
      if (dataRecords.length == 0) {
        break;
      }
      dataRecords.sort((a, b) => b.id - a.id);
      currentId = dataRecords[dataRecords.length - 1].id;
      context.log(
        `dataRecords ids: ${dataRecords[0].id} - ${
          dataRecords[dataRecords.length - 1].id
        }`
      );
      context.log("New current id: " + currentId);
      records.push(...dataRecords.filter((x) => x.id > maxId));
      context.log(`Page ${pageIndex} parsed`);
      pageIndex -= 1;
    }
  } else {
    for (let i = 1; i <= pages; i++) {
      context.log(`Getting page ${i}`);
      data = (await axios.get(generateUrl(i))).data;
      records.push(...data.data);
      context.log(`Page ${i} parsed`);
    }
  }
  context.log(`Records found: ${records.length}`);
  // convert records to flat records
  const flattenRecords = records.map((x) => flattenObject(x));
  if (flattenRecords.length == 0) {
    return;
  }
  const columns = await snowflake.createOrUpdateTable(
    tableName,
    flattenRecords,
    columnsConfig,
    forceCreateTable
  );
  await snowflake.insert(flattenRecords, tableName, columns, forceCreateTable);

  context.res = {
    status: 200 /* Defaults to 200 */,
  };
};

// module.exports(console, {});
