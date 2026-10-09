# W2: IMIX (60/590/1514 bytes in a 7:4:1 ratio) over a fixed number of
# concurrent connections, with a small share of SYNs that open them.
#
#   trex-console > start -f w2_imix.py -m 100% -t flows=1000000,syn_ratio=0.01,vip=10.0.0.100
from trex_stl_lib.api import *
import argparse

IMIX = [(60, 7), (590, 4), (1514, 1)]


class W2:
    def _stream(self, vip, flows, size, flags, weight):
        clients = max(1, flows // 60000 + 1)
        ports = min(60000, flows)
        base = Ether() / IP(src="16.0.0.1", dst=vip) / TCP(sport=1025, dport=80, flags=flags)
        pad = max(0, size - 4 - len(base)) * "x"
        vm = STLScVmRaw([
            STLVmTupleGen(ip_min="16.0.0.1", ip_max=ipaddress_from_int(0x10000000 + clients),
                          port_min=1025, port_max=1025 + ports - 1, name="tuple"),
            STLVmWrFlowVar(fv_name="tuple.ip", pkt_offset="IP.src"),
            STLVmWrFlowVar(fv_name="tuple.port", pkt_offset="TCP.sport"),
            STLVmFixIpv4(offset="IP"),
        ], cache_size=255)
        return STLStream(packet=STLPktBuilder(pkt=base / pad, vm=vm), mode=STLTXCont(pps=weight))

    def get_streams(self, tunables, **kwargs):
        parser = argparse.ArgumentParser()
        parser.add_argument("--flows", type=int, default=1000000)
        parser.add_argument("--syn_ratio", type=float, default=0.01)
        parser.add_argument("--vip", default="10.0.0.100")
        args = parser.parse_args(tunables)
        streams = []
        for size, weight in IMIX:
            streams.append(self._stream(args.vip, args.flows, size, "A", weight * (1.0 - args.syn_ratio)))
        streams.append(self._stream(args.vip, args.flows, 60, "S", sum(w for _, w in IMIX) * args.syn_ratio))
        return streams


def ipaddress_from_int(v):
    return ".".join(str((v >> s) & 0xFF) for s in (24, 16, 8, 0))


def register():
    return W2()
