package paxoskv

import "errors"

var (
	NotEnoughQuorum = errors.New("not enough quorum")
	AcceptorBasePort = int64(3333)
)

func (a *BallotNum) GE(b *BallotNum) bool {

}