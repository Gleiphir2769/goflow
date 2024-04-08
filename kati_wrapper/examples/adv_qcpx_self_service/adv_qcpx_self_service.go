package adv_qcpx_self_service

import (
	"time"

	"github.com/s8sg/goflow/kati_wrapper"
)

var GlobalEngine = &kati_wrapper.FlowEngine{}

func init() {
	wf := AdvQCPXSelgService()
	if err := GlobalEngine.Register("qcpx_service", wf); err != nil {
		panic(err)
	}
}

func AdvQCPXSelgService() kati_wrapper.WorkflowV1 {
	wf := kati_wrapper.WorkflowV1{}
	wf.AddLoader(&QCPXLoader{}).
		AddAssembler(&QCPXAssembler{})
	return wf
}

type QCPXAssembler struct {
}

func (d *QCPXAssembler) Assemble(ctx *kati_wrapper.GlobalContext) *kati_wrapper.ModelInfo {
	qcpxSelfServiceInfo := ctx.Get("qcpx_self_service").(map[int64]*QcpxSelfServiceInfo)

	if qcpxInfo, ok := qcpxSelfServiceInfo[ctx.Ad.AdvertiserId]; ok {
		RitRoiMap := make(map[int64]float32)
		for rit, roi := range qcpxInfo.RitRoiMap {
			RitRoiMap[rit] = float32(roi)
		}
		res := &QcpxSelfServiceInfo{
			Id:            qcpxInfo.Id,
			Amount:        qcpxInfo.Amount,
			ConsumeAmount: qcpxInfo.ConsumeAmount,
			OmpActivityId: qcpxInfo.OmpActivityId,
			Type:          qcpxInfo.Type,
			UpdateTime:    int64(time.Now().Unix()),
			RitRoiMap:     RitRoiMap,
		}
		assembleRes := workflow.NewAssembleRes("RerankerModel", "QcpxSelfServiceInfo", res)
		modelCh <- assembleRes
	}
	close(modelCh)
}

type QCPXLoader struct {
}

func (s *QCPXLoader) Load(ctx *kati_wrapper.GlobalContext) (any, error) {
	ads := ctx.AdContexts()

	adv_ids := make(map[int64]bool, len(ads))
	for _, ad := range ads {
		if ad.Ad.GetCreateChannel() == adcommon.CreateChannel_ecp && ad.Ad.GetQcpxMode() == constant.QcpxMode_ACTIVATED {
			adv_ids[ad.Ad.GetAdvertiserId()] = true
		}
	}
	res := make(map[int64]*QcpxSelfServiceInfo, len(adv_ids))
	for id := range adv_ids {
		req := hub.GetUserQcpxBenefitRequest{
			AdvId: id,
			Base:  &base.Base{},
		}
		rsp, err := getQcpxInfoByAdvid(&req)
		if err != nil {
			return nil, err
		}
		qcpxDetail := rsp.GetUserQcpxBenefit()
		userRoiMap := rsp.GetUserRoiMap()
		if qcpxDetail != nil && userRoiMap != nil {
			qcpxRet := QcpxSelfServiceInfo{
				Amount:        qcpxDetail.GetAmount(),
				ConsumeAmount: qcpxDetail.GetConsumeAmount(),
				Id:            qcpxDetail.GetId(),
				OmpActivityId: qcpxDetail.GetOmpActivityId(),
				Type:          qcpxDetail.GetType(),
				RitRoiMap:     userRoiMap,
			}
			res[id] = &qcpxRet
		}
	}

	return res, nil
}

func getQcpxInfoByAdvid(req *hub.GetUserQcpxBenefitRequest) (*hub.GetUserQcpxBenefitResponse, error) {
	rsp, err := caller.QcpxSelfServiceClient.GetUserQcpxBenefit(context.Background(), req)
	return rsp, err
}

func (s *QCPXLoader) IsSync() bool {
	return true
}

type QcpxSelfServiceInfo struct {
	Id            int64             // 自助qcpx赠款id
	Amount        int64             // 广告主发券quota余额
	ConsumeAmount int64             // 广告主已消耗quota
	OmpActivityId int64             // 发券activity_id
	Type          int32             // quota类型，1=基础，2=额外
	UpdateTime    int64             // 更新时间
	RitRoiMap     map[int64]float64 // 不同rit的发券roi要求
}
