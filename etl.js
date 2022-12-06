const fs = require("fs")
const path = require("path")
const parseUrl = require("parse-url")
const gunzip = require("gunzip-file")
const doMongoImports = require("./mongo-import.js")
const { isValidJson, sleep } = require("./helpers.js")

const inputDir = "./input"
const extractedDir = "./extracted"
console.time("ETLjob")

// Extract
const extractFiles = async () => {
  // Get gzipped files as array
  const files = await fs.promises.readdir(inputDir)

  if (!fs.existsSync(extractedDir)) {
    fs.mkdirSync(extractedDir)
  }

  console.log("Total Files", files.length)

  const promises = files.map((file) => {
    return new Promise(async (resolve, reject) => {
      try {
        const fromPath = path.join(inputDir, file)
        const toPath = path.join(extractedDir, file).replace(".gz", "")
        const stat = await fs.promises.stat(fromPath)
        if (stat.isFile()) {
          resolve(gunzip(fromPath, toPath))
        } else {
          reject(`${fromPath} is not a file!`)
        }
      } catch (e) {
        console.log("Error", e)
        reject(e)
      }
    })
  })

  const results = await Promise.allSettled(promises)
  const succeeded = results.filter((result) => result.status === "fulfilled")
  const failed = results.filter((result) => result.status === "rejected")

  return {
    success_extract: succeeded.length,
    failed_extract: failed.length,
  }
}

// Transform All Files
const transformAllFiles = async () => {
  const jsonFiles = await fs.promises.readdir(extractedDir)
  const promises = jsonFiles.map((file) => {
    return new Promise(async (resolve, reject) => {
      const fromPath = path.join(extractedDir, file)
      const fileContents = await fs.promises.readFile(fromPath)
      const jsonData = JSON.parse(fileContents)
      await transformData(jsonData)
        .then((data) => {
          resolve(data)
        })
        .catch(() => reject("bad transform"))
    })
  })

  const results = await Promise.allSettled(promises)
  const succeeded = results.filter((result) => result.status === "fulfilled")
  const failed = results.filter((result) => result.status === "rejected")
  const data = succeeded.flatMap((s) => s.value)

  console.log({
    success_transform: succeeded.length,
    failed_transform: failed.length,
  })
  return data
}

// Parse/Transform Event Data
const transformData = async (jsonData) => {
  if (!isValidJson(jsonData)) throw Error("bad data")
  if (!jsonData?.e?.length > 0) throw Error("no events")

  const urlObj = parseUrl(jsonData?.u)
  const url = {
    d: urlObj?.resource,
    p: urlObj?.pathname,
    q: urlObj?.query,
    h: urlObj?.hash ? `#${encodeURIComponent(urlObj?.hash)}` : "",
  }
  const parsedEvents = jsonData.e.map((ev) => {
    return {
      ts: jsonData?.ts,
      u: url,
      ec: ev,
    }
  })

  return parsedEvents
}

;(async () => {
  await extractFiles()
    .then((stats) => {
      console.log(stats)
    })
    .catch(() => {
      throw Error("Error extracting json files from gz")
    })

  // I should have used a pipline to transform a readable stream so not I/O bound... next time!
  await sleep(200)

  const dataToLoad = await transformAllFiles()
  if (!isValidJson(dataToLoad))
    throw Error("Transformed data is not valid JSON")

  fs.writeFileSync(`./DataForMongo.json`, JSON.stringify(dataToLoad))
  console.timeEnd("ETLjob")
  console.log("Done extracting!")
  await doMongoImports()
})()
