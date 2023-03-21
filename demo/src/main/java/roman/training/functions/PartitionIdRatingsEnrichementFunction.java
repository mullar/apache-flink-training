package roman.training.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import roman.training.domain.Rating;

public class PartitionIdRatingsEnrichementFunction extends RichMapFunction<Rating, Rating> {
    private transient ValueState<Long> partitionId;

    public Rating map(Rating value) throws Exception {
        if ( partitionId.value() == null) {
            partitionId.update(1L);
        } else {
            partitionId.update(partitionId.value() + 1L);
        }

        value.setPartitionId(partitionId.value());
        return value;
    };

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<Long>(
                        "partitionId", // the state name
                        TypeInformation.of(new TypeHint<Long>() {}));
        partitionId = getRuntimeContext().getState(descriptor);
    }    
}
