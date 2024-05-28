const { gql, GraphQLClient } = require("graphql-request");
const {
  initSnowflakeConnection,
  executeSql,
  initTable,
  insertRecords,
  flattenObject,
  getColumnsConfig,
} = require("../snowflake");

const client = new GraphQLClient("https://kinhome.enerflo.io/graphql", {
  headers: {
    authorization:
      "bearer c42ed5acf0114730fd4130d1a19f7e816ae7985249cd1f9d47d4958b55f1690c",
  },
});

const fetchDeals = async () => {
  let page = 1;
  const records = [];
  const tableName = "ENERFLO_DEALS";
  const connection = await initSnowflakeConnection();
  const [columnsConfig, forceCreateTable] = await getColumnsConfig(
    connection,
    "ENERFLO",
    tableName
  );
  let lastDate;

  if (!forceCreateTable) {
    try {
      const qLastDate = await executeSql(
        connection,
        `SELECT MAX(UPDATED_AT) FROM ${tableName}`
      );
      lastDate = qLastDate[0]?.["MAX(UPDATED_AT)"];
    } catch (err) {}
  }

  while (1) {
    const document = gql`
      query MyQuery($page: Float!, $date: String!) {
        fetchDealList(
          input: {
            pageSize: 300
            page: $page
            filter: { updatedAt: { _gt: $date } }
            orderBy: { by: ASC, field: "updatedAt" }
          }
        ) {
          currentPage
          pageSize
          totalItemCount
          totalPageCount
          items {
            createdAt
            updatedAt
            currentStage
            currentSubStage
            dispositionNotes
            customer {
              firstName
              lastName
              id
              email
            }
            disposition {
              id
              label
              status
            }
            id
            progress
            shortCode
            state
            status
            templateVersionIsValid
            updatedAt
            writable
            org {
              id
              name
              subdomain
              status
            }
            installer {
              id
              name
              status
              isActive
            }
            utilityBills {
              id
              fileUpload {
                bucket
                contentType
                isPublic
                createdAt
                key
                region
                size
                updatedAt
                uploadedAt
                signedUrl
                originalFilename
                id
              }
            }
          }
        }
      }
    `;
    const { fetchDealList: res } = await client.request(document, {
      page,
      date: lastDate ?? "2000-01-01",
    });
    records.push(...res.items);

    if (!res.totalPageCount || res.currentPage === res.totalPageCount) {
      break;
    }
    page += 1;
  }

  if (!records.length) {
    console.log("No records to insert");
    return;
  }
  console.log(`${records.length} records to insert`);

  for (const obj of records) {
    if (obj.utilityBills && obj.utilityBills.length) {
      obj.utilityBills = obj.utilityBills[0];

      for (const key of Object.keys(obj.utilityBills.fileUpload)) {
        obj.utilityBills[`fileUpload_${key}`] =
          obj.utilityBills.fileUpload[key];
      }
      delete obj.utilityBills.fileUpload;
    } else {
      delete obj.utilityBills;
    }
  }
  const flattenRecords = records.map((x) => flattenObject(x));
  const columns = await initTable(
    connection,
    "ENERFLO",
    tableName,
    flattenRecords,
    columnsConfig,
    forceCreateTable
  );
  await insertRecords(connection, flattenRecords, tableName, columns);
};

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  try {
    await fetchDeals();

    context.res = {
      status: 200 /* Defaults to 200 */,
    };
  } catch (err) {
    console.error(err);
    throw err;
  }
};
