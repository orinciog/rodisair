var fs=require('fs');
var path=require('path');
var http=require('http');
var async=require('async');

if (process.argv.length!=3)
{
	console.log("usage: node parse.js dir");
	process.exit();
}

var dirName=process.argv[2];
fs.readdir(dirName,doParseAllFiles);

var hospitals={};

function doParseAllFiles(err,files)
{
	async.eachSeries(files,doParseFileName,function(err){
		console.log(hospitals);
	});
}

function doRequest(hospitalName,callback)
{
	var searchQuery="/maps/api/geocode/json?address="+encodeURIComponent(hospitalName)+"&key=";
	//console.log("Send: "+ searchQuery);
	 http.get({
        host: 'maps.googleapis.com',
        path: searchQuery
    }, function(response) {
        // Continuously update stream with data
        var body = '';
        response.on('data', function(d) {
            body += d;
        });
        response.on('end', function() {

            // Data reception is done, do whatever with it!
            var parsed = JSON.parse(body);

            var response={lat:0,long:0};
            if (parsed.results.length>0)
            {
            	response.lat=parsed.results[0].geometry.location.lat;
            	response.long=parsed.results[0].geometry.location.lng;
            }
            callback(null,response);
        });
    });
}

function doParseFileName(fileName,callback)
{
	//IM_DRG___ALBA --- AB01 --- Spitalul Judetean de Urgenta Alba Iulia___1.1.2012_31.1.2012.xls
	var fileNameTmp=path.basename(fileName);
	fileNameTmp=fileNameTmp.substring(0,fileNameTmp.length-4);
	var parts=fileNameTmp.split(/-+/);
	var partsCounty=parts[0].split(/_+/);
	var partsHospital=parts[2].split(/_+/);

	var fileNameProperties={};
	fileNameProperties.county=partsCounty[2].trim();
	fileNameProperties.hospitalCode=parts[1].trim();
	fileNameProperties.hospitalName=partsHospital[0].trim();
	fileNameProperties.hospitalGeoPoint=[];

	doRequest(fileNameProperties.hospitalName,function(err,response){
		fileNameProperties.hospitalGeoPoint=[response.long,response.lat];
		//console.log("response: "+ JSON.stringify(response));
		hospitals[fileNameProperties.hospitalCode]=fileNameProperties;
		callback();
	});
}

