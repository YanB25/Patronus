#include "Common.h"

void steeringWithMacUdp(ibv_qp *qp, RdmaContext *ctx, const uint8_t mac[6], 
        uint16_t dstPort, uint16_t srcPort) {

    struct raw_eth_flow_attr {
        struct ibv_flow_attr        attr;
        struct ibv_flow_spec_eth    spec_eth;
        struct ibv_flow_spec_tcp_udp spec_udp;
    } __attribute__((packed)) flow_attr = {
        .attr = {
            .comp_mask  = 0,
            .type       = IBV_FLOW_ATTR_NORMAL,
            .size       = sizeof(flow_attr),
            .priority   = 0,
            .num_of_specs  = 2,
            .port       = ctx->port,
            .flags      = 0,
        },
        .spec_eth = {
            .type   = IBV_FLOW_SPEC_ETH,
            .size   = sizeof(struct ibv_flow_spec_eth),
            .val = {
                .dst_mac = {mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]},
                .src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                .ether_type = 0,
                .vlan_tag = 0,
            },
            .mask = {
                .dst_mac = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
                .src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                .ether_type = 0,
                .vlan_tag = 0,
            }
        },
        .spec_udp = {
            .type = IBV_FLOW_SPEC_UDP,
            .size = sizeof(struct ibv_flow_spec_tcp_udp),
            .val = {
                .dst_port = dstPort,
                .src_port = srcPort,
            },
            .mask = {
                .dst_port = 0xFFFF,
                .src_port = 0xFFFF,

            }
        },
    };

    struct ibv_flow *eth_flow;
    
    /* create steering rule */
    eth_flow = ibv_create_flow(qp, &flow_attr.attr);
    if (!eth_flow) {
        Debug::notifyError("Couldn't attach steering flow");
    }

}
