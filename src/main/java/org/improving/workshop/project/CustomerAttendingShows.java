package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import static org.improving.workshop.Streams.SERDE_TICKET_JSON;

@Slf4j
public class CustomerAttendingShows {
    public static final JsonSerde<VenueAddress> SERDE_VENUE_ADDRESS_JSON =
            new JsonSerde<>(CustomerAttendingShows.VenueAddress.class);

    public static final JsonSerde<ArrayList<Customer>> SERDE_FINAL_CUSTOMER_DETAILS_JSON =
            new JsonSerde<>(ArrayList.class);

    public static final String OUTPUT_TOPIC = "customer-attending-shows-output";


    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {



        KTable<String, Customer> customerTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_CUSTOMER_JSON)
                );

        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );


        var addressOfCustomerTable =  builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .filter((k,v) -> v.customerid() != null)
                .selectKey((key, value) -> value.customerid())
                .toTable(Named.as("customer-only-address"),
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("customer-only-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON));

        var addressOfVenueTable =  builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                //.peek((k,v) -> log.info("Value of address of venue before filter data k - {} and v - {}", k, v))
                .filter((k,v) -> v.customerid() == null)
                //.peek((k,v) -> log.info("Value of address of venue after filter data k - {} and v - {}", k, v))
                .toTable(Named.as("venue-only-address"),
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("venue-only-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON));

        var venueAndAddressTable = builder
                .stream(TOPIC_DATA_DEMO_VENUES, Consumed.with(Serdes.String(), SERDE_VENUE_JSON))
                //.peek((k,v) -> log.info("Value of venue table data k - {} and v - {}", k, v))
                .selectKey((k,v) -> v.addressid())
                //.peek((k,v) -> log.info("Value of venue table data k - {} and v - {}", k, v))
                .join(addressOfVenueTable, (addressId, venue, address) -> new VenueAddress(venue, address))
                //.peek((k,v) -> log.info("Value of venue and address table after join data k - {} and v - {}", k, v))
                .selectKey((k,v) -> v.venue.id())
                .toTable(Named.as("venue-and-address"),
                        Materialized
                                .<String, VenueAddress>as(persistentKeyValueStore("venue-and-address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_ADDRESS_JSON));

        builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((k,v)-> log.info("Entering Stream with key - {} and value - {}", k, v))
                .selectKey((k,v) -> v.customerid())
                .join(customerTable, (customerId, ticket, customer) -> new TicketCustomer(ticket, customer))
                .peek((k,v)-> log.info("Stream after joining customer table - {} and value - {}", k, v))
                .join(addressOfCustomerTable, (customerId, ticketCustomer, addressOfCustomer) -> new TicketCustomerAddress(ticketCustomer, addressOfCustomer))
                .peek((k,v)-> log.info("Stream after joining address of customer table - {} and value - {}", k, v))
                .selectKey((k, v) -> v.ticketCustomer.ticket.eventid())
                .join(eventsTable, (eventId, ticketCustomerAddress, event) -> new TicketCustomerAddressEvent(ticketCustomerAddress, event))
                .peek((k,v)-> log.info("Stream after joining events table - {} and value - {}", k, v))
                .selectKey((k, v) -> v.event.venueid())
                .join(venueAndAddressTable, (venueId, ticketCustomerAddressEvent, venueAndAddress) -> new TicketCustomerAddressEventVenueAndAddress(ticketCustomerAddressEvent, venueAndAddress))
                .peek((k,v)-> log.info("Stream after joining venue and Address table - {} and value - {}", k, v))
                .selectKey((k, v) -> v.ticketCustomerAddressEvent.ticketCustomerAddress.addressOfCustomer.state()+v.ticketCustomerAddressEvent.event.id())
                .filter((k,v) -> !v.ticketCustomerAddressEvent.ticketCustomerAddress.addressOfCustomer.state().equals(v.venueAddress.address.state()))
                .peek((k,v)-> log.info("Stream after filtering out different state customers - {} and value - {}", k, v))
                .groupByKey()
                .aggregate(ArrayList<Customer>::new,
                        (key, oldValue, list)->{
                            log.info("Customers in aggregate - {} and list - {}", oldValue.ticketCustomerAddressEvent.ticketCustomerAddress.ticketCustomer.customer, list);
                            list.add(oldValue.ticketCustomerAddressEvent.ticketCustomerAddress.ticketCustomer.customer);
                            return list;
                        })
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_FINAL_CUSTOMER_DETAILS_JSON));

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VenueAddress {
        private Venue venue;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomer {
        private Ticket ticket;
        private Customer customer;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddress {
        private TicketCustomer ticketCustomer;
        private Address addressOfCustomer;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddressEvent {
        private TicketCustomerAddress ticketCustomerAddress;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TicketCustomerAddressEventVenueAndAddress {
        private TicketCustomerAddressEvent ticketCustomerAddressEvent;
        private VenueAddress venueAddress;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FinalCustomerDetails {
        private Customer customer;
    }
}
