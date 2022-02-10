var request = require('request');
var options = {
    'method': 'GET',
    'url': 'https://cms.megaphone.fm/api/networks/acee2874-d42e-11eb-b8c4-63db778ddb9e/podcasts/a33b8688-d8ec-11eb-9594-47c7acb499cb/episodes',
    'headers': {
        'Token': 'token="d06f4c2fe896ad513246545f7ca7c053"',
        'podcast_id': 'acee2874-d42e-11eb-b8c4-63db778ddb9e',
        'Authorization': 'Bearer d06f4c2fe896ad513246545f7ca7c053'
    }
};
request(options, function (error, response) {
    if (error) throw new Error(error);
    console.log(response.body);
});