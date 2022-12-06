const fs = require("fs")
const mongoose = require("mongoose")
mongoose.set("strictQuery", false)
require("dotenv").config({ silent: true })
const eventSchema = new mongoose.Schema({
  ts: {
    type: Date,
    required: true,
  },
  u: {
    type: Object,
    required: true,
  },
  ec: {
    type: Object,
    required: true,
  },
})
const Event = mongoose.model("events", eventSchema)
console.time("MongoImportJob")

// Connect to Mongo
connectToDb = async () => {
  const connection_string = `${process.env.MONGO_HOST}`
  await mongoose
    .connect(connection_string, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    })
    .then(() => {
      console.log(`Connected to MongoDB`)
    })
    .catch((err) => {
      console.log(`Error connecting to MongoDB ${err}`)
      process.exit(1)
    })
}

const importData = async (events) => {
  try {
    await Event.create(events)
    console.log("Data added!")
    console.timeEnd("MongoImportJob")
  } catch (e) {
    console.error(e)
  }
}

module.exports = doMongoImports = async () => {
  await connectToDb()

  const events = JSON.parse(fs.readFileSync(`./DataForMongo.json`, "utf-8"))

  if (events && events.length > 0)
    await importData(events).finally(() => process.exit())
}
