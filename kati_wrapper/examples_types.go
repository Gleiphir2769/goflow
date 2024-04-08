package kati_wrapper

type RerankerModel struct {
	// ...
	QcpxSelfServiceInfo *QcpxSelfServiceInfo
	// ...
}

type QcpxSelfServiceInfo struct {
	Id            int64             `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Amount        int64             `protobuf:"varint,2,opt,name=amount,proto3" json:"amount,omitempty"`
	ConsumeAmount int64             `protobuf:"varint,3,opt,name=consume_amount,json=consumeAmount,proto3" json:"consume_amount,omitempty"`
	OmpActivityId int64             `protobuf:"varint,4,opt,name=omp_activity_id,json=ompActivityId,proto3" json:"omp_activity_id,omitempty"`
	Type          int32             `protobuf:"varint,5,opt,name=type,proto3" json:"type,omitempty"`
	UpdateTime    int64             `protobuf:"varint,6,opt,name=update_time,json=updateTime,proto3" json:"update_time,omitempty"`
	RitRoiMap     map[int64]float32 `protobuf:"bytes,7,rep,name=rit_roi_map,json=ritRoiMap" json:"rit_roi_map,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"fixed32,2,opt,name=value,proto3"`
}
