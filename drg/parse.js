var xls = require('xlsjs');
var async = require('async');
var path = require('path');
var elasticsearch = require('elasticsearch');
var config=require('./config.js');
var hospitals=require('./hospitals.js')
var md5 = require('MD5');

var es_client = new elasticsearch.Client({
	host:"http://localhost:9200",
	requestTimeout: 60000
});

if (process.argv.length!=3)
{
	console.log("usage: node parse.js file");
	process.exit();
}

var objects = [];
var diases = [];
var fileNameProperties = {};

var fileName=process.argv[2];

async.series({
    one: function(callback){
        doParseFileName(callback);
    },
    two: function(callback){
       doParseFile(callback);
    },
    uploadObjects: function(callback){
       doUploadResults(objects,callback);
    },
    /*
    uploadDiases: function(callback){
       doUploadResults(diases,callback);
    }*/
},function (err)
{
	if (err)
	{
		console.log("Error: "+err.message);
	}
	else
		console.log("File "+fileName + " parsed succesfully");
	process.exit();
});

function doParseFileName(callback)
{
	//IM_DRG___ALBA-IULIA --- AB01 --- Spitalul Judetean de Urgenta Alba-Iulia___1.1.2012_31.1.2012.xls
	var fileNameTmp=path.basename(fileName);
	fileNameTmp=fileNameTmp.substring(0,fileNameTmp.length-4);
	var parts=fileNameTmp.split(/_+/);

	var startDateParts=parts[parts.length-2].trim().split('.');
	var endDateParts=parts[parts.length-1].trim().split('.');

	var partsCode=fileNameTmp.split('---');

	fileNameProperties.hospitalCode=partsCode[1].trim();
	
	var stD=new Date(startDateParts[2],parseInt(startDateParts[1])-1,startDateParts[0]);
	stD=new Date(stD.getTime()+2*60*60*1000);
	var enD=new Date(endDateParts[2],parseInt(endDateParts[1])-1,endDateParts[0]);
	enD=new Date(enD.getTime()+2*60*60*1000);
	fileNameProperties.startDate=stD.toISOString();
	fileNameProperties.endDate=enD.toISOString();
	fileNameProperties.period=enD.getTime()-stD.getTime();

	if (hospitals.hospitals[fileNameProperties.hospitalCode])
	{
		var json = hospitals.hospitals[fileNameProperties.hospitalCode];
		for (var key in json)
			fileNameProperties[key] = json[key];
	}
	callback();
}

function doParseFile(callback)
{
	var workbook = xls.readFile(fileName);
	var sheet_name_list = workbook.SheetNames;
	var ws=workbook.Sheets[sheet_name_list[0]];
	var range=xls.utils.decode_range(ws['!ref']);

	for(var R = 14; R <= range.e.r-1; ++R) 
	{
		var jsonNewObject = {};
		for(var C = range.s.c; C <= range.e.c; ++C) 
		{
			var cell_range = {c:C, r:R};
			var col_address=xls.utils.encode_col(cell_range.c);
			//console.log("Cell range: "+JSON.stringify(cell_range));
			//console.log("Col address: "+JSON.stringify(col_address));

			if (config.jsonObjectDRG[col_address])
			{
				var key=config.jsonObjectDRG[col_address];
				var cell_address = xls.utils.encode_cell(cell_range);
				//console.log("Cell address: "+JSON.stringify(cell_address));
				
				var value = "";
				if (ws[cell_address])
				{
					value = ws[cell_address].v;
				}
				else if (col_address>="G")
					value=0;

				if (col_address=="B" && !isNumber(value))
					break;
				if (col_address=="F")
				{
					var newDiases={};
					var codeNameDiases=config.jsonObjectDRG['D'];
					newDiases[codeNameDiases]=jsonNewObject[codeNameDiases];
					newDiases[config.jsonObjectDRG['F']]=value;
					addNewDiases(newDiases);
				}
				else				
					jsonNewObject[key]=value;
				//console.log("Key = "+key+" Value = "+value);
			}
		}
		if (Object.keys(jsonNewObject).length>0)
			doRowWork(jsonNewObject);
	}

	callback(null);
}

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function addNewDiases(newDiases)
{
	var bulkOp= { "index" : { "_index" : "medicalrecords", "_type" : "diases","_id": md5(newDiases.diasesCode) } };

	diases.push(bulkOp);
	diases.push(newDiases);
}

function doRowWork(jsonObject)
{
	var bulkOp= { "index" : { "_index" : "medicalrecords", "_type" : "drg" } };
	for (var key in fileNameProperties)
	{
		jsonObject[key]=fileNameProperties[key];
	}
	objects.push(bulkOp);
	objects.push(jsonObject);
}


function doUploadResults(bulkObjects,callback)
{
	if (bulkObjects.length==0)
	{
		callback(null);
		return;
	}
	//console.log(bulkObjects);
	//callback();
	es_client.bulk({body:bulkObjects},callback);
}
