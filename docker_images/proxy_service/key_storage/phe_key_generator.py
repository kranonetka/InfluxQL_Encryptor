import pickle

from paillier.keygen import generate_keys

pk, sk = generate_keys(k=128)

with open("phe_public_key", "wb") as pub:
    pickle.dump(pk, pub)

with open("phe_private_key", "wb") as priv:
    pickle.dump(sk, priv)
