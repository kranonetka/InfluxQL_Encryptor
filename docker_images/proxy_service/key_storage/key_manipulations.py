import json
from hashlib import sha256
from pathlib import Path

from paillier import keygen

_root = Path(__file__).parent


def generate_keys(phe_bits=128):
    phe_pk, phe_sk = keygen.generate_keys(phe_bits)
    
    ope = sha256(str(phe_sk.mu + phe_sk.lam).encode()).digest()
    
    return phe_pk, phe_sk, ope


def dump_keys(phe_pk: keygen.PublicKey, phe_sk: keygen.SecretKey, ope: bytes):
    keypair = dict(
        phe_pk=[
            phe_pk.n,
            phe_pk.g
        ],
        phe_sk=[
            phe_sk.lam,
            phe_sk.mu
        ],
        ope=ope.hex()
    )
    with (_root / 'keys.json').open('w') as fp:
        json.dump(keypair, fp, indent=4)


def load_keys():
    with (_root / 'keys.json').open('r') as fp:
        keys = json.load(fp)
    
    pk, sk = keys['phe_pk'], keys['phe_sk']
    ope = keys['ope']
    
    return keygen.PublicKey(pk), keygen.SecretKey(sk), bytes.fromhex(ope)
