const _ = require('lodash')
const { DateTime, Interval } = require('luxon')
const mongoose = require('mongoose')

let agencyKey

let Stops
let Routes
let Trips
let StopTimes
let Calendars
let CalendarDates

exports.configure = (config) => {
  agencyKey = config.agencyKey
}

exports.setConnection = (conn) => {
  Stops = conn.model('Stops')
  Routes = conn.model('Routes')
  Trips = conn.model('Trips')
  StopTimes = conn.model('StopTimes')
  Calendars = conn.model('Calendars')
  CalendarDates = conn.model('CalendarDates')
}

exports.getCalendars = async () => {
  let calenders = await Calendars.find({ agency_key: agencyKey }).exec()
  return calenders
}

exports.getCalendarDates = async () => {
  let dates = await CalendarDates.find({ agency_key: agencyKey }).exec()
  return dates
}

exports.getServiceCodes = async (searchDate) => {
  let dayName = searchDate.toFormat('EEEE').toLowerCase()
  let calendars = await exports.getCalendars()
  let serviceIds = []
  console.log(`there are ${calendars.length} calendars`)
  calendars.forEach(item => {
    // Sometimes GTFS data contains rows with no actual data,
    // We can ignore those.
    if (item.service_id) {
      // also ensure that today is within the valid date range
      let startDate = DateTime.fromFormat(item.start_date.toString(), 'yyyyLLdd')
      let endDate = DateTime.fromFormat(item.end_date.toString(), 'yyyyLLdd')
      let interval = Interval.fromDateTimes(startDate, endDate)

      if (item[dayName] && interval.contains(searchDate)) {
        serviceIds.push(item.service_id)
      }
    }
    // TODO: Indicate if there is no calendar dates
  })

  // add service IDs for calendar-dates
  let calendarDates = await exports.getCalendarDates()
  if (calendarDates.length) {
    calendarDates.forEach(item => {
      if (item.service_id) {
        let date = DateTime.fromFormat(item.date.toString(), 'yyyyLLdd')
        if (+date === +searchDate) {
          if (item.exception_type === 1) { // INCLUDE service on this date
            serviceIds.push(item.service_id)
          } else if (item.exception_type === 2) { // EXCLUDE service from this date
            // log(item.service_id)
            _.pull(serviceIds, item.service_id)
          }
        }
      }
    })
  }

  return serviceIds
}

exports.getAllStops = async () => {
  let stops = await Stops.find(
    { agency_key: agencyKey }
  ).exec()

  return stops
}

exports.getStopIdFromStopCode = async (stopCode) => {
  // stopCode = toString(stopCode)
  let stops = await Stops.find(
    { stop_code: stopCode, agency_key: agencyKey },
    'stop_id'
  ).exec()
  let stopId = stops[0].stop_id
  return stopId
}

exports.getRoutes = async (routeNames = []) => {
  // If route names is undefinied, make it an empty array
  let query = routeNames.length
    ? { route_short_name: { $in: routeNames }, agency_key: agencyKey }
    : { agency_key: agencyKey }
  let routes = await Routes.find(query, 'route_id route_short_name').exec()
  return routes
}

exports.getRoutesFromIDs = async (routeIDs = []) => {
  let objIds = routeIDs.map(x => mongoose.Types.ObjectId(x))
  let query
  if (routeIDs.length) {
    query = {
      _id: {
        $in: objIds
      },
      agency_key: agencyKey
    }
  } else {
    query = {
      agency_key: agencyKey
    }
  }

  let routes = await Routes.find(query, 'route_id route_short_name').exec()
  return routes
}

exports.getTripsFromRouteNames = async (serviceIds, routes) => {
  let trips
  if (routes) { // only filter by routenames if the caller passed in routenames
    let routeIds = _.map(routes, 'route_id')
    trips = await Trips.find(
      {
        route_id: { $in: routeIds },
        service_id: { $in: serviceIds },
        agency_key: agencyKey
      },
      'trip_id route_id service_id trip_headsign'
    ).exec()
  } else {
    trips = await Trips.find({
      service_id: { $in: serviceIds },
      agency_key: agencyKey
    }, 'trip_id route_id service_id trip_headsign'
    ).exec()
  }

  return trips
}

exports.getStopTimesFromTrips = async (stopId, serviceIds, trips) => {
  let query = {
    stop_id: stopId,
    // pickup_type: 0, // for some reason the new WTA times don't have pickup types
    agency_key: agencyKey
  }

  if (trips) {
    query['trip_id'] = { $in: _.map(trips, 'trip_id') }
  }

  let stopTimes = await StopTimes.find(query, 'trip_id departure_time').exec()

  stopTimes = _.sortBy(stopTimes, s =>
    DateTime.fromFormat(s.departure_time, 'H:mm:ss')
  )
  return stopTimes
}
