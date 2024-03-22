package org.improving.workshop.project

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.improving.workshop.exercises.stateless.AddressSortAndStringify
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification

class CustomerAttendingShowsSpec extends Specification {

    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Address> addressInputTopic
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Customer> customerInputTopic
    TestInputTopic<String, Venue> venueInputTopic

    // outputs - addressid, address (string)
    TestOutputTopic<String, String> outputTopic


    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the AddressSortAndStringify topology (by reference)
        CustomerAttendingShows.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        )

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        )

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        )

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                CustomerAttendingShows.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "customers from same state attending event in a different state"() {
        given: 'a set of customers'
        def cust1 = new Customer("cust-123", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02")
        def cust2 = new Customer("cust-456", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02")
        def cust3 = new Customer("cust-567", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02")

        and: 'address of these customers'
        def address1 = new Address("addr-123", "cust-123", "cd", "HOME", "111 1st St", "Apt 2", "Madison", "WI", "55555", "1233", "USA", 0L, 0L)
        def address2 = new Address("addr-456", "cust-456", "cd", "HOME", "111 1st St", "Apt 2", "Dallas", "TX", "55553", "1234", "USA", 0L, 0L)
        def address3 = new Address("addr-567", "cust-567", "cd", "HOME", "111 21st St", "Apt 3", "Madison", "WI", "55554", "1235", "USA", 0L, 0L)

        and: 'address of events'
        def address4 = new Address("addr-789", null, "cd", "HOME", "111 1st St", "Apt 2", "Austin", "TX", "55556", "1236", "USA", 0L, 0L)

        and: 'venues of events'
        def venue1 = new Venue("ven-123", "addr-789", "Armoury", 1000)

        and: 'event happening'
        def event1 = new Event("ev-123", "artist-2", "ven-123", 10, "today")

        and: 'Tickets of customers'
        def ticket1 = new Ticket("tkt-123", "cust-123", "ev-123", 200)
        def ticket2 = new Ticket("tkt-456", "cust-456", "ev-123", 200)
        def ticket3 = new Ticket("tkt-456", "cust-567", "ev-123", 200)


        when: 'piping inputs through the stream'
        customerInputTopic.pipeInput(cust1.id(), cust1)
        customerInputTopic.pipeInput(cust2.id(), cust2)
        customerInputTopic.pipeInput(cust3.id(), cust3)

        addressInputTopic.pipeInput(address1.id(), address1)
        addressInputTopic.pipeInput(address2.id(), address2)
        addressInputTopic.pipeInput(address3.id(), address3)

        addressInputTopic.pipeInput(address4.id(), address4)

        venueInputTopic.pipeInput(venue1.id(), venue1)

        eventInputTopic.pipeInput(event1.id(), event1)



        ticketInputTopic.pipeInput(ticket1.id(), ticket1)
        ticketInputTopic.pipeInput(ticket2.id(), ticket2)
        ticketInputTopic.pipeInput(ticket3.id(), ticket3)

        and: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 2
    }
}

