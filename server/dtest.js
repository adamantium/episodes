var http = require('http');

var options = {
	host: 'dev.clique.kr',
	port: 8000,
	path: '/episode/index',
	method: 'POST'
};

var req = http.request(options, function (res) {
	console.log('STATUS: ' + res.statusCode);
	console.log('HEADERS: ' + JSON.stringify(res.headers));
	res.setEncoding('utf8');
	res.on('data', function (chunk) {
		console.log('BODY: ' + chunk);
	});
});

req.write('dummy1');
req.end();