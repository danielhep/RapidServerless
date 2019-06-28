'use strict'
const mongoose = require('mongoose')
const { DateTime, Interval } = require('luxon')
const _ = require('lodash')

const dbLookup = require('./src/dbLookups.js')

const Stops = require('gtfs/models/gtfs/stop')
const Routes = require('gtfs/models/gtfs/route')
const Trips = require('gtfs/models/gtfs/trip')
const StopTimes = require('gtfs/models/gtfs/stop-time')
const Calendars = require('gtfs/models/gtfs/calendar')
const CalendarDates = require('gtfs/models/gtfs/calendar-date')

const agencyKey = 'nctd'

const mongoURI = 'mongodb+srv://readonly:1sv5GULN0Fp5WXd5@cluster0-dguyv.azure.mongodb.net/gtfs?retryWrites=true&w=majority'

let conn = null

dbLookup.configure({ agencyKey })

let connectToDatabase = async function (context) {
  context.callbackWaitsForEmptyEventLoop = false
  console.log('Creating connection.')
  if (conn == null) {
    conn = await mongoose.createConnection(mongoURI, {
      // Buffering means mongoose will queue up operations if it gets
      // disconnected from MongoDB and send them when it reconnects.
      // With serverless, better to fail fast if not connected.
      bufferCommands: false, // Disable mongoose buffering
      bufferMaxEntries: 0, // and MongoDB driver buffering
      connectTimeoutMS: 1000
    })
  } else {
    console.log('Already connected!')
  }
  conn.model('Stops', Stops.schema)
  conn.model('Routes', Routes.schema)
  conn.model('Trips', Trips.schema)
  conn.model('StopTimes', StopTimes.schema)
  conn.model('Calendars', Calendars.schema)
  conn.model('CalendarDates', CalendarDates.schema)
  console.log('Connected')
}

module.exports.getStops = async (event, context) => {
  await connectToDatabase(context)

  if (conn) console.log('Definitely connected, getting stops')

  let stops = await conn.model('Stops').find(
    { agency_key: agencyKey }
  ).exec()

  return {
    statusCode: 200,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true
    },
    body: JSON.stringify(stops, null, 2)
  }
}

module.exports.getRoutes = async (event, context) => {
  await connectToDatabase(context)

  if (conn) console.log('Definitely connected, getting routes')

  let routes = await conn.model('Routes').find(
    { agency_key: agencyKey }
  ).exec()

  return {
    statusCode: 200,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true
    },
    body: JSON.stringify(routes, null, 2)
  }
}

module.exports.getScheduleByStop = async (event, context) => {
  await connectToDatabase(context)

  dbLookup.setConnection(conn)

  let routeIds = event.multiValueQueryStringParameters.routes
  let stopId = event.multiValueQueryStringParameters.stop
  let date = DateTime.fromISO(event.multiValueQueryStringParameters.date)

  console.log(`We got stop ID: ${stopId}`)

  let stop = await conn.model('Stop').findById(stopId).exec()

  let serviceIds = await dbLookup.getServiceCodes(date)
  // get a list of routes from the route names
  let routes = await dbLookup.getRoutesFromIDs(routeIds)
  let trips = await dbLookup.getTripsFromRouteNames(serviceIds, routes)

  let stopTimes = await dbLookup.getStopTimesFromTrips(stop.stop_id, serviceIds, trips)

  let departures = []
  let lastTime
  if (stopTimes.length) {
    // there might be no stops here
    lastTime = DateTime.fromFormat(stopTimes[0].departure_time, 'H:mm:ss')
  }
  stopTimes.forEach((stopTime, ind) => {
    let trip = _.find(trips, { trip_id: stopTime.trip_id })
    let routeId = trip.route_id
    let tripHeadsign = trip.trip_headsign

    let route = _.find(routes, { route_id: routeId })
    let routeName = route.route_short_name

    let time = DateTime.fromFormat(stopTime.departure_time, 'H:mm:ss')
    let spacing = (ind)
      ? Interval.fromDateTimes(lastTime, time).length('seconds')
      : undefined
    lastTime = time

    departures.push({
      routeId,
      routeName,
      time: time.toISOTime(),
      spacing,
      tripHeadsign
    })
  })

  return {
    statusCode: 200,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true
    },
    body: JSON.stringify(departures, null, 2)
  }
}
