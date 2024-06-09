const { gql, GraphQLClient } = require("graphql-request");
const { flattenObject, Snowflake } = require("../snowflake");

const client = new GraphQLClient("https://kinhome.enerflo.io/graphql", {
  headers: {
    authorization:
      "bearer c42ed5acf0114730fd4130d1a19f7e816ae7985249cd1f9d47d4958b55f1690c",
  },
});

const fetchCustomers = async (context) => {
  let page = 1;
  const records = [];

  const snowflake = await Snowflake.create(context);
  const tableName = "ENERFLO_CUSTOMERS";
  const [columnsConfig, forceCreateTable] = await snowflake.getColumnsConfig(
    tableName
  );

  let lastDate;

  if (!forceCreateTable) {
    try {
      const qLastDate = await snowflake.execute(
        `SELECT MAX(UPDATED_AT) FROM ${tableName}`
      );
      lastDate = qLastDate[0]?.["MAX(UPDATED_AT)"];
    } catch (err) {}
  }

  while (1) {
    const document = gql`
      query MyQuery($page: Float!, $date: String!) {
        fetchCustomerList(
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
            id
            firstName
            lastName
            mobile
            language
            email
            createdAt
            address {
              city
              country
              lat
              line1
              line2
              line3
              lng
              postalCode
              state
            }
            phone
            phoneIsMobile
            status
            tz
            updatedAt
          }
        }
      }
    `;
    const { fetchCustomerList: res } = await client.request(document, {
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
    context.log("No records to insert");
    return;
  }
  context.log(`${records.length} records to insert`);

  const flattenRecords = records.map((x) => flattenObject(x));
  const columns = await snowflake.createOrUpdateTable(
    tableName,
    flattenRecords,
    columnsConfig,
    forceCreateTable
  );
  await snowflake.insert(flattenRecords, tableName, columns, forceCreateTable);
};

module.exports = async function (context, myTimer) {
  var timeStamp = new Date().toISOString();

  if (myTimer.isPastDue) {
    context.log("JavaScript is running late!");
  }
  context.log("JavaScript timer trigger function ran!", timeStamp);

  try {
    await fetchCustomers(context);

    context.res = {
      status: 200 /* Defaults to 200 */,
    };
  } catch (err) {
    context.error(err);
    throw err;
  }
};

// module.exports(console, {});
