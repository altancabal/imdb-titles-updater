'use strict';

console.log("Starting the application...")

require('dotenv').config()
const uuidv1 = require('uuid/v1');
var sleep = require('system-sleep');
const zlib = require('zlib');
var request = require('request');
const fs = require('fs');

console.log("Downloading the IMDb database...")
request('https://datasets.imdbws.com/title.akas.tsv.gz').pipe(fs.createWriteStream('tmp.tsv.gz'))
sleep(5000);

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: process.env.ES_HOST,
  log: process.env.ES_LOG_LEVEL,
  apiVersion: process.env.ES_API_VERSION
});

var Promise = require('bluebird');

client.ping({
  requestTimeout: 30000,
}, function (error) {
  if (error) {
    console.error('elasticsearch cluster is down!');
  } else {
    console.log('Ping was successfull');
  }
});

console.log("Dropping index...");
Promise.resolve().then(dropIndex())
sleep(5000);
console.log("Creating index...");
Promise.resolve().then(createIndex())
sleep(5000);

var lineReader = require('readline').createInterface({
	input: fs.createReadStream('tmp.tsv.gz').pipe(zlib.createGunzip())
});

async function addBulkToIndex(bulkBody) {
  return client.bulk({
	  body: bulkBody
	});
}

/*lineReader.on('line', function (line) {
	//console.log(line);
	var w = line.split("\t");
	Promise.resolve().then(addToIndex(w));
	//var response = addToIndex(w);
});*/

var count = 0;
var totalCount = 0;
var bb = []; //Bulk body
lineReader.on('line', function (line) {
	count += 1;
	totalCount += 1;
	line = line.replace(/\\N/g, "");
	line = line.replace(/\\/g, "\\\\");
	line = line.replace(/"/g, "\\\"");
	var w = line.split("\t");
	if(count >= 10000){
		console.log("Total count: "+totalCount);
		sleep(1500);
		count = 0;
		Promise.resolve().then(addBulkToIndex(bb));
		bb = [];
	}
	bb.push(readJson("{\"index\":{\"_index\":\"titles_aka\",\"_type\":\"title\",\"_id\":\""+uuidv1()+"\"}}\n"))
	
	if(totalCount > 1){
		var lines = "{";
		lines += "\"titleId\":\""+w[0]+"\",";
		lines += "\"ordering\":"+w[1]+",";
		lines += "\"title\":\""+w[2]+"\",";
		lines += "\"region\":\""+w[3]+"\",";
		lines += "\"language\":\""+w[4]+"\",";
		//lines += "\"types\":\""+w[5]+"\",";
		//lines += "\"attributes\":\""+w[6]+"\",";
		if(w[7].length > 0){
			lines += "\"isOriginalTitle\":"+w[7];
		}
		else{
			lines = lines.substring(0, lines.length-1)
		}
		lines += "}\n";
		bb.push(readJson(lines));
	}
});

function readJson (data) {
    var obj = JSON.parse(data);
    return(obj)
 };




// FUNCTIONS
function dropIndex() {
  return client.indices.delete({
    index: 'titles_aka',
  });
}

function createIndex() {
  return client.indices.create({
    index: 'titles_aka',
	body:{
	mappings: {
		title: {
		  properties: {
			titleId: {
			  type: "keyword"
			},
			ordering: {
			  type: "integer"
			},
			title: {
			  type: "keyword"
			},
			region: {
			  type: "keyword"
			},
			language: {
			  type: "keyword"
			},
			types: {
			  type: "keyword"
			},
			attributes: {
			  type: "keyword"
			},
			isOriginalTitle: {
			  type: "integer"
			}
		  }
		}
	  }
	}
  });
}

/*async function addToIndex(w) {
  return await client.index({
	  index: 'titles_aka',
	  type: 'title',
	  body: {
		titleId: w[0],
		ordering: w[1],
		title: w[2],
		region: w[3],
		language: w[4],
		types: w[5],
		attributes: w[6],
		isOriginalTitle: w[7]
	  }
	});
}*/

console.log("Deleting temporary files...")
fs.unlink('tmp.tsv.gz')
console.log("Application ended")