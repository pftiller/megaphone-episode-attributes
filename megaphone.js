require('dotenv').config();
let moment = require('moment');
var request = require('request');
const {BigQuery} = require('@google-cloud/bigquery');
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
        let dataToAdd = [];
        function createRecord(item) {
            return {
                "id": item.id,
                "title": item.title,
                "pubdate": moment(item.pubdate).format('YYYY-MM-DD'),
                "episodeType": item.episodeType,
                "seasonNumber": item.seasonNumber,
                "episodeNumber": item.episodeNumber,
                "summary": item.summary,
                "duration": item.duration,
                "uid": item.uid,
                "podcastId": item.podcastId,
                "preCount": item.preCount,
                "postCount": item.postCount,
                "pubdateTimezone": item.pubdateTimezone,
                "originalFilename": item.originalFilename,
                "draft": item.draft,
                "podcastTitle": item.podcastTitle,
                "podcastItunesCategories": item.podcastItunesCategories,
                "mainFeed": item.mainFeed,
                "adFree": item.adFree
            }
        }
        request({
            'method': 'GET',
            'url': `${process.env.NETWORK_API_URL}/${program.megaphone_id}/episodes`,
            'headers': {
                'Token': `token="${process.env.TOKEN}"`,
                'Authorization': `Bearer ${process.env.TOKEN}`
            }
        }, function (error, response) {
            if (error) {
                reject(error)
            }
            else {
                var episodes = JSON.parse(response.body);
                    for (var i in episodes) {
                        var obj = createRecord(episodes[i]);
                        dataToAdd.push(obj)
                    }
                        
                };
                resolve(dataToAdd);
            });
            // console.log(response.body);
            // var data = JSON.stringify(response.body)
            // var data = JSON.parse(data);
            // function myfunc(data) {
            //     for (var i in data) {
            //         dataToAdd.push(data[i].title 
            //                );
            //     }
            //     console.log(dataToAdd)
            //     resolve(dataToAdd);
            // }
            
           
        });
    
}
   let dataArray = [];
   programs.forEach(async (program) => {
       let episode = parseRSS(program)
       dataArray.push(episode);
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

