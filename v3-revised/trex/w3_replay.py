# W3: replay of recorded service traces with TRex's stateful (ASTF) engine.
# Each captured flow is replayed as a real TCP connection towards the VIP at
# <cps> new connections per second.
#
#   ./t-rex-64 -i --astf                           # in one shell
#   trex-console > start -f w3_replay.py -m 1 -t pcap=/data/traces/video.pcap,cps=2000,vip=10.0.0.100
from trex.astf.api import *
import argparse


class W3:
    def get_profile(self, tunables, **kwargs):
        parser = argparse.ArgumentParser()
        parser.add_argument("--pcap", required=True)
        parser.add_argument("--cps", type=float, default=1000.0)
        parser.add_argument("--vip", default="10.0.0.100")
        parser.add_argument("--clients", default="16.0.0.1-16.0.255.254")
        args = parser.parse_args(tunables)
        c_min, c_max = args.clients.split("-")
        ip_gen = ASTFIPGen(
            glob=ASTFIPGenGlobal(ip_offset="1.0.0.0"),
            dist_client=ASTFIPGenDist(ip_range=[c_min, c_max], distribution="seq"),
            dist_server=ASTFIPGenDist(ip_range=[args.vip, args.vip], distribution="seq"),
        )
        return ASTFProfile(default_ip_gen=ip_gen, cap_list=[ASTFCapInfo(file=args.pcap, cps=args.cps)])


def register():
    return W3()
