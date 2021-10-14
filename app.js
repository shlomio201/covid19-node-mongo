const https = require('https');
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const AWS = require('aws-sdk');

const s3 = new AWS.S3({
    accessKeyId: 'AKIA5BVUM4JY5MIWC6FL',
    secretAccessKey: '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'
});

const url = "mongodb+srv://covid:CdiTvy1XHjGi25Wx@covid19.qdnu0.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
const client = new MongoClient(url);

const jsonurl = "https://covid.ourworldindata.org/data/owid-covid-data.json";

main().catch(console.error);

async function main() {
    console.log("Starting download")
    const result = await loadJson(jsonurl)
    const arry = Object.entries(result);
    console.log('Total: ' + arry.length +' Docs.')
    const half_length = Math.ceil(arry.length / 2);
    let left = []
    let right = [];
    let i = 0;
    arry.forEach(element => {

        const tmpobj = {
            "country_code": element[0],
            ...element[1]
        };
        if (i < half_length) {
            left.push(tmpobj)
        } else {
            right.push(tmpobj)
        }
        i++;
    });
    console.log('Left: ' + left.length + ', Right: ' + right.length)
    console.log("Uploading file to db")
    await upload(left/*.splice(0, 3)*/).catch(console.dir); //remove splice before sending!!!!!!!
    console.log("Half way there...")
    await upload(right/*.splice(0, 3)*/).catch(console.dir);
    console.log("File uploaded to DB secsesfuly")

    const countries = await getcount();
    console.log("Total countries in DB: " + countries.length)
    fs.writeFile('newfile.csv', 'Total nuber of countries in DB: ' + countries.length, function (err) {
        if (err) throw err;
        console.log('File created locally successfully');
    });
    console.log('Uploading file to s3');
    await uploadFile('newfile.csv')
}


async function loadJson(url) {
    let getdata = new Promise(function (resolve) {
        https.get(url, (res) => {
            let body = "";
            res.on("data", (chunk) => {
                body += chunk;
            });
            res.on("end", () => {
                try {
                    resolve(JSON.parse(body));
                } catch (error) {
                    console.error(error.message);
                };
            });
        }).on("error", (error) => {
            console.error(error.message);
        });
    })
    let json = await getdata;
    console.log('Download completed')
    return json;
}

async function upload(docs) {
    try {
        await client.connect();
        const database = client.db("covidadb");
        const collection = database.collection("covidcollection");
        collection.createIndex({ "country_code": 1 }, { unique: true })
        const options = { ordered: false };
        const result = await collection.insertMany(docs, options);
        console.log(`${result.insertedCount} documents were inserted`);
    } finally {
        await client.close();
    }
}


async function getcount() {
    try {
        await client.connect();
        const database = client.db("covidadb");
        const collection = database.collection("covidcollection");
        return await collection.distinct('country_code')
    } finally {
        await client.close();
    }
}

const uploadFile = async (fileName) => {
    fs.readFile(fileName, (err, data) => {
        if (err) throw err;
        const params = {
            Bucket: 'covid19shlomi',
            Key: fileName,
            Body: JSON.stringify(data, null, 2)
        };
        s3.upload(params, function (s3Err, data) {
            if (s3Err) throw s3Err
            console.log(`File uploaded successfully!  ${data.Location}`)
            console.log('Waiting for your call, shlomi :) ');
        });
    });
};

