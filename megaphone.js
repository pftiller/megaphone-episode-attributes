let Parser = require('rss-parser');
let moment = require('moment');
const {BigQuery} = require('@google-cloud/bigquery');
let parser = new Parser();
const programs = [
    {
      "program": "Brains On! Science podcast for kids",
      "megaphone_id": "b3809b1a-d8eb-11eb-9594-ff8d32426b1f"
    },
    {
      "program": "Corner Office from Marketplace",
      "megaphone_id": "9dfb917c-d8ec-11eb-82dc-478483ad60ca"
    },
    {
      "program": "Don't Ask Tig",
      "megaphone_id": "a33b8688-d8ec-11eb-9594-47c7acb499cb"
    },
    {
      "program": "Forever Ago",
      "megaphone_id": "a741f206-04ea-11ec-9d4c-179e2e0aa82b"
    },
    {
      "program": "How We Survive",
      "megaphone_id": "4ac90870-17be-11ec-9fb9-db104c25036c"
    },
    {
      "program": "In Front of Our Eyes",
      "megaphone_id": "e4bf1690-04ea-11ec-9a60-77baeacb40b9"
    },
    {
      "program": "In the Dark",
      "megaphone_id": "a755ccce-d8ec-11eb-acbe-abe0c32365c1"
    },
    {
      "program": "Julie�s Library",
      "megaphone_id": "ba47cec2-d8ec-11eb-bda0-8b1c5a25968a"
    },
    {
      "program": "Make Me Smart with Kai and Molly",
      "megaphone_id": "be89f6e0-d8ec-11eb-b321-c335a536ec66"
    },
    {
      "program": "Marketplace",
      "megaphone_id": "ccc1747c-d8ec-11eb-b5e2-6ba46cb92864"
    },
    {
      "program": "Marketplace Morning Report",
      "megaphone_id": "c5bb0044-d8ec-11eb-a09a-931a443d4364"
    },
    {
      "program": "Marketplace Tech",
      "megaphone_id": "c95d2948-d8ec-11eb-bfab-87156cf77488"
    },
    {
      "program": "Million Bazillion",
      "megaphone_id": "d07a7208-d8ec-11eb-8fa6-a3366b4257db"
    },
    {
      "program": "Moment of Um",
      "megaphone_id": "2192513e-2856-11ec-954b-7b9363df6c97"
    },
    {
      "program": "Smash Boom Best",
      "megaphone_id": "d47b0476-d8ec-11eb-a5a6-eb7d03078ea5"
    },
    {
      "program": "Spectacular Failures",
      "megaphone_id": "d81aa622-d8ec-11eb-8fa6-5bc3a53a8937"
    },
    {
      "program": "TBTL- Too Beautiful to Live",
      "megaphone_id": "ecd859f6-d8ec-11eb-9594-afe14f723909"
    },
    {
      "program": "Terrible, Thanks For Asking",
      "megaphone_id": "dbf82b0c-d8ec-11eb-a8a4-57ab97704e15"
    },
    {
      "program": "The Slowdown",
      "megaphone_id": "df9e552e-d8ec-11eb-a820-2b3bb9f01b42"
    },
    {
      "program": "The Splendid Table",
      "megaphone_id": "e31a5374-d8ec-11eb-bfab-5b0369ff83f1"
    },
    {
      "program": "The Uncertain Hour",
      "megaphone_id": "e655f61a-d8ec-11eb-a5a6-b31d48af82bf"
    },
    {
      "program": "This Is Uncomfortable",
      "megaphone_id": "e9e60248-d8ec-11eb-8716-27ff4700152c"
    }
   ];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
    projectId: projectId
});
const datasetId = 'apm_podcasts';
const tableId = 'episode_titles';
async function insertRowsAsStream(param) {
    let rows = param;
    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
    return 'Ok';
}
let removeDups = async () => {
    let sqlQuery = `CREATE OR REPLACE TABLE ${projectId}.${datasetId}.${tableId} AS SELECT Episode, uri_path, Program, Title FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Program, Episode, uri_path) row_number FROM ${projectId}.${datasetId}.${tableId} ) WHERE row_number = 1`;
    const options = {
        query: sqlQuery,
        location: 'US'
    };
    const [rows] = await bigquery.query(options);
    console.log(`Table is now ${rows.length} rows`);
}
let parseRSS = (url) => {
    return new Promise((resolve, reject) => {
        let dataToAdd = [];
        let parseUri = new RegExp('rss\/o(.*)');
        function createRecord(url, item) {
            return {
                program: url.program,
                title: item.title,
                uri_path: null,
                episode: moment(item.pubDate).format('YYYY-MM-DD'),
            //     getUri() {
            //         return this.uri_path = parseUri.exec(item.enclosure.url);
            //     }
            }
        }
        parser.parseURL(url.feed, (err, feed) => {
            if (err) {
                reject(err);
            } else if (feed.title = url.program) {
                feed.items.forEach(item => {
                    if (item.hasOwnProperty('enclosure')) {
                        var obj = createRecord(url, item);
                        // var obj2 = obj.getUri();
                        // obj.uri_path = obj2[1];
                        // delete obj.getUri;
                        dataToAdd.push(obj)
                    }
                    resolve(dataToAdd);
                });
            }
        });
    })
}
   let dataArray = [];
   urls.forEach(async (url) => {
       let feed = parseRSS(url)
       dataArray.push(feed);
   })
    module.exports = (() => {
        Promise.all(dataArray).then((data) => {
            console.log('got the data', data);
            data.forEach((datae) => {
                insertRowsAsStream(datae).then((res) => {
                    if (res = 'Ok') {
                        removeDups()
                        .then(data =>{
                        console.log('did it', res);
                        })
                        .catch(e => {
                            console.log(e)
                        })
                    }
                    }).catch((err)=>{
                        console.log(err);
                })  
            })
        })
    })
