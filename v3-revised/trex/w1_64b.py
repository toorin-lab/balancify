# W1: 64-byte TCP packets over a fixed number of concurrent connections.
#
# Two streams share one tuple generator over <flows> (client IP, port) pairs:
# a SYN stream that opens connections at <syn_ratio> of the packet rate and a
# data (ACK) stream for the rest, so the table holds the overridden subset of
# <flows> connections while every packet exercises the lookup path.
#
#   ./t-rex-64 -i                                  # in one shell
#   trex-console > start -f w1_64b.py -m 25mpps -t flows=1000000,syn_ratio=0.01,vip=10.0.0.100
from trex_stl_lib.api import *
import argparse


class W1:
    def _stream(self, vip, flows, flags, pps_share):
        clients = max(1, flows // 60000 + 1)
        ports = min(60000, flows)
        base = Ether() / IP(src="16.0.0.1", dst=vip) / TCP(sport=1025, dport=80, flags=flags)
        pad = max(0, 60 - len(base)) * "x"
        vm = STLScVmRaw([
            STLVmTupleGen(ip_min="16.0.0.1", ip_max=str(ipaddress_from_int(0x10000000 + clients)),
                          port_min=1025, port_max=1025 + ports - 1, name="tuple"),
            STLVmWrFlowVar(fv_name="tuple.ip", pkt_offset="IP.src"),
            STLVmWrFlowVar(fv_name="tuple.port", pkt_offset="TCP.sport"),
            STLVmFixIpv4(offset="IP"),
        ], cache_size=255)
        return STLStream(packet=STLPktBuilder(pkt=base / pad, vm=vm),
                         mode=STLTXCont(pps=pps_share))

    def get_streams(self, tunables, **kwargs):
        parser = argparse.ArgumentParser()
        parser.add_argument("--flows", type=int, default=1000000)
        parser.add_argument("--syn_ratio", type=float, default=0.01)
        parser.add_argument("--vip", default="10.0.0.100")
        args = parser.parse_args(tunables)
        return [
            self._stream(args.vip, args.flows, "S", args.syn_ratio),
            self._stream(args.vip, args.flows, "A", 1.0 - args.syn_ratio),
        ]


def ipaddress_from_int(v):
    return ".".join(str((v >> s) & 0xFF) for s in (24, 16, 8, 0))


def register():
    return W1()
