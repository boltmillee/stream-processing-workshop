package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
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
public class CustomerStreamingArtist {

    public static final JsonSerde<CustomerIdList> SERDE_CUSTOMER_ID_DETAILS_JSON =
            new JsonSerde<>(CustomerIdList.class);

    public static final JsonSerde<CustomerIdListStateArtist> SERDE_FINAL_JSON =
            new JsonSerde<>(CustomerIdListStateArtist.class);

    public static final String OUTPUT_TOPIC_ARTIST = "customer-streaming-artist-output";

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

        KTable<String, Artist> artistTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
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

        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .selectKey((key, value) -> value.customerid())
                .join(customerTable, (customerId, stream, customer) -> new CustomerStream(stream, customer))
                .join(addressOfCustomerTable, (customerId, customerStream, customerAddress) -> new CustomerStreamAddress(customerStream, customerAddress))
                .groupBy((k,v)->v.customerStream.stream.artistid()+"-"+v.customerAddress.state())
                //.groupByKey()
                .aggregate(CustomerIdList::new,
                        (key, oldValue, list) -> {
                            //log.info("Customers in aggregate - {} and list - {}", oldValue.);
                            list.customerIds.add(oldValue.customerAddress.customerid());

                            return list;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, CustomerIdList>as(persistentKeyValueStore("customer-list-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_CUSTOMER_ID_DETAILS_JSON))
                .toStream()
                .map((k,v) -> KeyValue.pair(k.split("-")[0], new CustomerIdListState(v.customerIds, k.split("-")[1])))
                .join(artistTable, (artistId, customerIdListState, artist) -> new CustomerIdListStateArtist(customerIdListState, artist))
                .to(OUTPUT_TOPIC_ARTIST, Produced.with(Serdes.String(), SERDE_FINAL_JSON));


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerIdListStateArtist {
        private CustomerIdListState customerIdListState;
        private Artist artist;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerIdListState {
        private List<String> customerIds;
        private String state;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerIdList {
        private List<String> customerIds = new ArrayList<>();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStream {
        private Stream stream;
        private Customer customer;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStreamAddress {
        private CustomerStream customerStream;
        private Address customerAddress;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStreamAddressArtist {
        private CustomerStreamAddress customerStreamAddress;
        private Artist artist;
    }

//    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
//    public static class FinalCustomerDetails {
//        private Customer customer;
//    }

}