package paxos

import (
	"golang.org/x/net/context"
	"errors"
	"fmt"
	"github.com/kr/pretty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"sync"
	"time"
)

var (
	NotEnoughQuorum = errors.New("not enough quorum")
	AcceptorBasePort = int64(3333)
)

func (a *BallotNum) GE(b *BallotNum) bool {
	if a.N > b.N {
		return true
	}
	if a.N < b.N {
		return false
	}
	return a.ProposerId >= b.ProposerId
}

func (p *Proposer) Phase1(acceptorIds []int64, quorum int) (*Value, *BallotNum, error) {

	replies := p.rpcToAll(acceptorIds, "Prepare")

	ok := 0
	higherBal := *p.Bal
	maxVoted := &Acceptor{VBal: &BallotNum{}}

	for _, r := range replies {
		pretty.Logf("Proposer: handling Prepare reply: %s", r)
		if !p.Bal.GE(r.LastBal) {
			if r.LastBal.GE(&higherBal) {
				higherBal = *r.LastBal
			}
			continue
		}

		if r.VBal.GE(maxVoted.VBal) {
			maxVoted = r
		}

		ok += 1
		if ok == quorum {
			return maxVoted.Val, nil, nil
		}
	}

	return nil, &higherBal, NotEnoughQuorum
}

func (p *Proposer) Phase2(acceptorIds []int64, quorum int) (*BallotNum, error) {

	replies := p.rpcToAll(acceptorIds, "Accept")

	ok := 0
	higherBal := *p.Bal
	for _, r := range replies {
		pretty.Logf("Proposer: handling Accept reply: %s", r)
		if !p.Bal.GE(&higherBal) {
			higherBal = *r.LastBal
		}
		continue
	}
	ok += 1
	if ok == quorum {
		return nil, nil
	}
	return &higherBal, NotEnoughQuorum
}

func (p *Proposer) rpcToAll(acceptorIds []int64, action string) []*Acceptor {

	replies := []*Acceptor{}

	for _, aid := range acceptorIds {
		var err error
		address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+int64(aid))
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}

		defer conn.Close()
		c := NewPaxosKVClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var reply *Acceptor
		if action == "Prepare" {
			reply, err = c.Prepare(ctx, p)
		} else if action == "Accept" {
			reply, err = c.Accept(ctx, p)
		}
		if err != nil {
			log.Printf("Proposer: %s failure from Acceptor-%d: %v", action, aid, err)
		}
		log.Printf("Proposer: recv %s reply from Acceptor-%d: %v", action, aid, reply)

		replies = append(replies, reply)
	}
	return replies
}

type Version struct {
	mu		sync.Mutex
	acceptor Acceptor
}

type Versions map[int64] *Version

type KVServer struct {
	mu 		sync.Mutex
	Storage map[string]Versions
}

//todo coding through writing outline
func (s *KVServer) getLockedVersion(id *PaxosInstanceId) *Version {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.Key
	ver := id.Ver
	rec, found := s.Storage[key]

	if !found {
		rec = Versions{}
		s.Storage[key] = rec
	}

	v, found := rec[ver]
	if !found {
		rec[ver] = &Version{
			acceptor: Acceptor{
				LastBal: &BallotNum{},
				VBal: &BallotNum{},
			},
		}
		v = rec[ver]
	}
	pretty.Logf("Acceptor: getLockedVersion: %s", v)
	//todo why lock?
	v.mu.Lock()

	return v
}

//todo  waste error
func (s *KVServer) Prepare(c context.Context, r *Proposer) (*Acceptor, error) {
	pretty.Logf("Acceptor: recv Prepare-request: %v", r)

	//todo read not write, do not add
	v := s.getLockedVersion(r.Id)
	defer v.mu.Unlock()

	reply := v.acceptor
	if r.Bal.GE(v.acceptor.LastBal) {
		v.acceptor.LastBal = r.Bal
	}

	return &reply, nil
}

// waste error
func (s *KVServer) Accept(c context.Context, r *Proposer) (*Acceptor, error) {
	pretty.Logf("Acceptor: recv Accept-request: %v", r)

	v := s.getLockedVersion(r.Id)
	defer v.mu.Unlock()

	d := *v.acceptor.LastBal
	reply := Acceptor{
		LastBal: &d,
	}

	if r.Bal.GE(v.acceptor.LastBal) {
		v.acceptor.LastBal = r.Bal
		v.acceptor.VBal = r.Bal
		v.acceptor.Val = r.Val
	}

	return &reply, nil
}

func ServeAcceptors(acceptorIds []int64) []*grpc.Server {

	servers := []*grpc.Server{}

	for _, aid := range acceptorIds {
		addr := fmt.Sprintf(":%d", AcceptorBasePort+int64(aid))

		listen, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("listen: %s, %v", addr, err)
		}
		s := grpc.NewServer()
		RegisterPaxosKVServer(s, &KVServer{
			Storage: map[string]Versions{},
		})
		reflection.Register(s)
		pretty.Logf("Acceptor-%d serving on %s ...", aid, addr)
		servers = append(servers, s)
		go s.Serve(listen)
	}
	return servers
}









