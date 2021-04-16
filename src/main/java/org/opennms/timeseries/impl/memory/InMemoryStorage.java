/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.timeseries.impl.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Simulates a time series storage in memory (Guava cache). The implementation is super simple and not very efficient.
 * For testing and evaluating purposes only, not for production.
 */
public class InMemoryStorage implements TimeSeriesStorage {

    private final ConcurrentMap<Metric, Collection<Sample>> data;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");

    public InMemoryStorage () {
        Cache<Metric, Collection<Sample>> cache = CacheBuilder.newBuilder().maximumSize(10000).build();
        data = cache.asMap();

        ConsoleReporter reporter = ConsoleReporter.forRegistry(this.getMetrics())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);
    }

    @Override
    public void store(final List<Sample> samples) {
        Objects.requireNonNull(samples);
        for(Sample sample : samples) {
            Collection<Sample> timeseries = data.computeIfAbsent(sample.getMetric(), k -> new ConcurrentLinkedQueue<>());
            timeseries.add(sample);
        }
        samplesWritten.mark(samples.size());
    }

    @Override
    public List<Metric> getMetrics(final Collection<Tag> tags) {
        Objects.requireNonNull(tags);
        return data.keySet().stream().filter(metric -> containsAll(metric, tags)).collect(Collectors.toList());
    }

    private boolean containsAll(final Metric metric, final Collection<Tag> tags) {
        for(Tag tag: tags) {
            if(!metric.getIntrinsicTags().contains(tag) && !metric.getMetaTags().contains(tag)){
                return false;
            }
        }
        return true;
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) {
        Objects.requireNonNull(request);
        if(request.getAggregation() != Aggregation.NONE) {
            throw new IllegalArgumentException(String.format("Aggregation %s is not supported.", request.getAggregation()));
        }

        if(!data.containsKey(request.getMetric())){
            return Collections.emptyList();
        }
        return data.get(request.getMetric()).stream()
                .filter(sample -> sample.getTime().isAfter(request.getStart()))
                .filter(sample -> sample.getTime().isBefore(request.getEnd()))
                .collect(Collectors.toList());
    }

    @Override
    public void delete(Metric metric) {
        Objects.requireNonNull(metric);
        this.data.remove(metric);
    }

    @Override
    public String toString() {
        return this.getClass().getName();
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }
}
