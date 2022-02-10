require('dotenv').config();
// let Parser = require('rss-parser');
var request = require('request');
const {BigQuery} = require('@google-cloud/bigquery');
// let parser = new Parser();
const programs =     [{
    "name": "Forever Ago",
    "megaphone_id": "a741f206-04ea-11ec-9d4c-179e2e0aa82b"
  },
  {
    "name": "How We Survive",
    "megaphone_id": "4ac90870-17be-11ec-9fb9-db104c25036c"
  }];
// const projectId = `apmg-data-warehouse`;
// const bigquery = new BigQuery({
//     projectId: projectId
// });
// const datasetId = 'apm_podcasts';
// const tableId = 'megaphone_episode_attributes';
// async function insertRowsAsStream(param) {
//     let rows = param;
//     await bigquery
//         .dataset(datasetId)
//         .table(tableId)
//         .insert(rows);
//     console.log(`Inserted ${rows.length} rows`);
//     return 'Ok';
// }
// let removeDups = async () => {
//     let sqlQuery = `CREATE OR REPLACE TABLE ${projectId}.${datasetId}.${tableId} AS SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode, uri_path) row_number FROM ${projectId}.${datasetId}.${tableId} ) WHERE row_number = 1`;
//     const options = {
//         query: sqlQuery,
//         location: 'US'
//     };
//     const [rows] = await bigquery.query(options);
//     console.log(`Table is now ${rows.length} rows`);
// }
let parseRSS = (program) => {
    return new Promise((resolve, reject) => {
        request({
            'method': 'GET',
            'url': `${process.env.NETWORK_API_URL}/${program.megaphone_id}/episodes`,
            'headers': {
                'Token': `token="${process.env.TOKEN}"`,
                // 'podcast_id': 'acee2874-d42e-11eb-b8c4-63db778ddb9e',
                'Authorization': `Bearer ${process.env.TOKEN}`
              }
            }, function (error, response) {
            if (error) throw new Error(error);
            console.log(response.body);
            resolve(response.body)
        });

    //     parser.parseprogram(program.feed, (err, feed) => {
    //         if (err) {
    //             reject(err);
    //         } else if (feed.title = program.program) {
    //             feed.items.forEach(item => {
    //                 if (item.hasOwnProperty('enclosure')) {
    //                     var obj = createRecord(program, item);
    //                     // var obj2 = obj.getUri();
    //                     // obj.uri_path = obj2[1];
    //                     // delete obj.getUri;
    //                     dataToAdd.push(obj)
    //                 }
    //                 resolve(dataToAdd);
    //             });
    //         }
    //     });
    })
}
   let dataArray = [];
   programs.forEach(async (program) => {
       let feed = parseRSS(program)
       dataArray.push(feed);
   })
    module.exports = (() => {
        Promise.all(dataArray).then((data) => {
            console.log('got the data', data);
            // data.forEach((datae) => {
            //     insertRowsAsStream(datae).then((res) => {
            //         if (res = 'Ok') {
            //             removeDups()
            //             .then(data =>{
            //             console.log('did it', res);
            //             })
            //             .catch(e => {
            //                 console.log(e)
            //             })
            //         }
            //         }).catch((err)=>{
            //             console.log(err);
            //     })  
            // })
        })
    })

