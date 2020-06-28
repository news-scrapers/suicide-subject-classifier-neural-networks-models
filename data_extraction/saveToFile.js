const MongoClient = require('mongodb').MongoClient;
require('dotenv').config()
const url = process.env["database_url"];
const fs = require("fs");

const pathoutbundle = "../data/extracted_tweets/"

MongoClient.connect(url, async function (err, db) {
  if (err) throw err;
  var dbo = db.db("tweets-scraping");


  console.log("Starting search with suicide")


  const searchPromisified = (query) => {
    return new Promise((resolve, reject)=>{
      dbo.collection("ScrapedTweets").find(query).toArray(function (err, result) {
        if (err) return reject(err)
        return resolve(result);
      })
    })
  }

  const results = []
  

  const resultsSearch = await searchPromisified({'scraper_id':'scraper_suicide'})
    

  for (const result of resultsSearch) {
    result["about_suicide"]=1    
  }
  const noRepetitionResults = resultsSearch
  const noRepertitionLength = noRepetitionResults.length
  

  let dataSuicide = JSON.stringify(noRepetitionResults);
  fs.writeFileSync(pathoutbundle + "suicide_tweets.json", dataSuicide);

  console.log("----------------")
  console.log("Starting search with no domestic violence words")


  //  { $match : { newspaper : "elpais" } },
  //  { $sample : { size: 3 } }
  //]
  const query = [{ $match : {"scraper_id":"scraper_news_es"} }, { $sample : { size: 2*noRepertitionLength } }]

  const searchAggragatePromisified = () => {
    return new Promise((resolve, reject)=>{
      dbo.collection("ScrapedTweets").aggregate(query, { allowDiskUse: true }).toArray(function (err, result) {
        if (err) reject(err)
        return resolve(result);
      })
    })
  }

  resultsNoViolencia = await searchAggragatePromisified()

  words = ["suici"]

  const aboutDv = (result) => {
    for (const word of words){
      if (result.content && result.content.toLowerCase().indexOf(word)>-1){
        console.log("removed new from list because talks about " + word)
        return true
      }
      return false
    }
  }

  const depuredListResultsNoViolencia = []

  for (const result of resultsNoViolencia) {
    if (result && !aboutDv(result)){
      result["about_suicide"]=0
      depuredListResultsNoViolencia.push(result)
    } else {
      result["about_suicide"]=1
    }
    
  }

  console.log("no suicide: found " + depuredListResultsNoViolencia.length)
  console.log("suicide found " + noRepetitionResults.length)

  let dataNoViolence = JSON.stringify(depuredListResultsNoViolencia);
  fs.writeFileSync(pathoutbundle + "no_suicide_tweets.json", dataNoViolence);


  db.close();

});