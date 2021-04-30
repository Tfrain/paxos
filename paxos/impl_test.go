package paxos

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAcceptor_Accept_Deref_LastBal(t *testing.T) {
	ta := require.New(t)

	kvs := KVServer{
		Storage: map[string]Versions{},
	}
	p := &Proposer{
		Id: &PaxosInstanceId{
			Key: "x",
			Ver: 0,
		},
		Bal: &BallotNum{N: -1},
	}

	reply, err := kvs.Accept(nil, p)
	_ = err
	v := kvs.Storage["x"][0]

	v.acceptor.LastBal.N = 100
	ta.Equal(int64(0), reply.LastBal.N)
}