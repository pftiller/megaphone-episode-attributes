import * as dotenv from 'dotenv'
dotenv.config();
import moment from 'moment';
import request from 'request';
import {BigQuery} from '@google-cloud/bigquery';
const programs = [{
        "name": "Brains On! Science podcast for kids",
        "megaphone_id": "b3809b1a-d8eb-11eb-9594-ff8d32426b1f"
    },
    {
        "name": "Corner Office from Marketplace",
        "megaphone_id": "9dfb917c-d8ec-11eb-82dc-478483ad60ca"
    },
    {
        "name": "Don't Ask Tig",
        "megaphone_id": "a33b8688-d8ec-11eb-9594-47c7acb499cb"
    },
    {
        "name": "Forever Ago",
        "megaphone_id": "a741f206-04ea-11ec-9d4c-179e2e0aa82b"
    },
    {
        "name": "How We Survive",
        "megaphone_id": "4ac90870-17be-11ec-9fb9-db104c25036c"
    },
    {
        "name": "In Deep",
        "megaphone_id": "1180160e-3bfd-11ec-abad-27e0f31930b3"
    },
    {
        "name": "In Front of Our Eyes",
        "megaphone_id": "e4bf1690-04ea-11ec-9a60-77baeacb40b9"
    },
    {
        "name": "In the Dark",
        "megaphone_id": "a755ccce-d8ec-11eb-acbe-abe0c32365c1"
    },
    {
        "name": "Julie's Library",
        "megaphone_id": "ba47cec2-d8ec-11eb-bda0-8b1c5a25968a"
    },
    {
        "name": "Make Me Smart",
        "megaphone_id": "be89f6e0-d8ec-11eb-b321-c335a536ec66"
    },
    {
        "name": "Marketplace",
        "megaphone_id": "ccc1747c-d8ec-11eb-b5e2-6ba46cb92864"
    },
    {
        "name": "Marketplace Minute",
        "megaphone_id": "761ff71c-9395-11ea-a7fd-2f9b30216283"
    },
    {
        "name": "Marketplace Minute News Briefing",
        "megaphone_id": "bd5ed85a-9395-11ea-9e25-9b7bcc6a824a"
    },
    {
        "name": "Marketplace Morning Report",
        "megaphone_id": "c5bb0044-d8ec-11eb-a09a-931a443d4364"
    },
    {
        "name": "Marketplace Tech",
        "megaphone_id": "c95d2948-d8ec-11eb-bfab-87156cf77488"
    },
    {
        "name": "Million Bazillion",
        "megaphone_id": "d07a7208-d8ec-11eb-8fa6-a3366b4257db"
    },
    {
        "name": "Moment of Um",
        "megaphone_id": "2192513e-2856-11ec-954b-7b9363df6c97"
    },
    {
        "name": "Mood Ring",
        "megaphone_id": "fc258cf4-3903-11ec-bd8d-3f89c2c3a5ca"
    },
    {
        "name": "Sent Away",
        "megaphone_id": "cefe1d76-3bfc-11ec-a0c9-1b3c5a41e210"
    },
    {
        "name": "Smash Boom Best",
        "megaphone_id": "d47b0476-d8ec-11eb-a5a6-eb7d03078ea5"
    },
    {
        "name": "Sold a Story",
        "megaphone_id": "372f0d9c-3bfd-11ec-a91a-63234074389e"
    },
    {
        "name": "Spectacular Failures",
        "megaphone_id": "d81aa622-d8ec-11eb-8fa6-5bc3a53a8937"
    },
    {
        "name": "TBTL- Too Beautiful to Live",
        "megaphone_id": "ecd859f6-d8ec-11eb-9594-afe14f723909"
    },
    {
        "name": "The Angry Therapist",
        "megaphone_id": "1b860306-ae3a-11ed-9403-27bd0cf88f61"
    },
    {
        "name": "The Antidote",
        "megaphone_id": "13c35864-3904-11ec-ae8c-f3441c60593e"
    },
    {
        "name": "The One Recipe",
        "megaphone_id": "3057d2f8-7256-11ec-9bdc-b3e1f05dd842"
    },
    {
        "name": "The Slowdown",
        "megaphone_id": "df9e552e-d8ec-11eb-a820-2b3bb9f01b42"
    },
    {
        "name": "The Splendid Table",
        "megaphone_id": "e31a5374-d8ec-11eb-bfab-5b0369ff83f1"
    },
    {
        "name": "The Uncertain Hour",
        "megaphone_id": "e655f61a-d8ec-11eb-a5a6-b31d48af82bf"
    },
    {
        "name": "This Is Uncomfortable",
        "megaphone_id": "e9e60248-d8ec-11eb-8716-27ff4700152c"
    },
    {
        "name": "Truth Be Told with Tonya Mosley",
        "megaphone_id": "86967988-c414-11ed-a252-a32a1f54908b"
    },
    {
        "name": "YourClassical Storytime",
        "megaphone_id": "9284cf58-a6f4-11ec-9738-b757d0ec2a87"
    }
];
const projectId = `apmg-data-warehouse`;
const bigquery = new BigQuery({
    projectId: projectId
});
const datasetId = 'apm_podcasts';
const tableId = 'megaphone_episode_attributes';
async function insertRowsAsStream(param) {
    let rows = param;
    await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows);
    console.log(`Inserted ${rows.length} rows`);
    return 'Ok';
}
let getEpisodes = (program) => {
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
                "mainFeed": item.mainFeed,
                "adFree": item.adFree
            }
        }
        request({
            'method': 'GET',
            'url': `${process.env.NETWORK_API_URL}/podcasts/${program.megaphone_id}/episodes?draft=false`,
            'headers': {
                'Token': `token="${process.env.TOKEN}"`,
                'Authorization': `Bearer ${process.env.TOKEN}`
            }
        }, function(error, response) {
            if (error) {
                reject(error)
            } else {
                var episodes = JSON.parse(response.body);
                for (var i in episodes) {
                    var obj = createRecord(episodes[i]);
                    dataToAdd.push(obj)
                }
            };
            resolve(dataToAdd);
        });
    });
}
let dataArray = [];
programs.forEach(async (program) => {
    let episode = getEpisodes(program)
    dataArray.push(episode);
})
export function fetchEpisodes() {
    Promise.all(dataArray).then((data) => {
        data.forEach((datae) => {
            if (datae.draft != true) {
                insertRowsAsStream(datae).then((res) => {
                    if (res = 'Ok') {
                        console.log('did it');
                    }
                }).catch((err) => {
                    console.log(err);
                })
            }
        })
    })
}