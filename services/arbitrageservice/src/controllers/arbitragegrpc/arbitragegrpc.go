package arbitragegrpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/alexkalak/go_market_analyze/services/arbitrageservice/src/arbitrageservice"
	pb "github.com/alexkalak/go_market_analyze/services/arbitrageservice/src/controllers/arbitragegrpc/proto"
	"google.golang.org/grpc"
)

type ArbitrageGRPCServer interface {
	Start() error
}

type ArbitrageGRPCServerConfig struct {
	Port uint
}

func (c *ArbitrageGRPCServerConfig) validate() error {
	if c.Port == 0 {
		return errors.New("ArbitrageGrpcServerConfig field Port cannot equal to 0")
	}

	return nil
}

type ArbitrageGRPCServerDependencies struct {
	ArbitrageService arbitrageservice.ArbitrageService
}

func (d *ArbitrageGRPCServerDependencies) validate() error {
	if d.ArbitrageService == nil {
		return errors.New("ArbitrageGrpcServerDependencies field ArbitrageService cannot be nil")
	}

	return nil
}

type arbitrageGRPCServer struct {
	arbitrageService arbitrageservice.ArbitrageService
	config           ArbitrageGRPCServerConfig
	pb.UnimplementedArbitrageServer
}

func New(config ArbitrageGRPCServerConfig, dependencies ArbitrageGRPCServerDependencies) (ArbitrageGRPCServer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	return &arbitrageGRPCServer{
		config:           config,
		arbitrageService: dependencies.ArbitrageService,
	}, nil
}

func (s *arbitrageGRPCServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterArbitrageServer(server, s)

	log.Println("🚀 gRPC server running on", s.config.Port)
	if err := server.Serve(lis); err != nil {
		fmt.Println("failed to serve: %v", err)
		return err
	}

	return nil
}

func (s *arbitrageGRPCServer) GetArbsOnChain(ctx context.Context, req *pb.ArbsOnChainRequest) (*pb.ArbsOnChainResponse, error) {
	arbs, err := s.arbitrageService.FindAllArbs(uint(req.ChainId))
	if err != nil {
		return nil, err
	}

	//FIX: add support for different ChainID changes
	result := pb.ArbsOnChainResponse{
		ChainId:    1,
		Arbitrages: []*pb.OnChainArbitrage{},
	}

	for _, arb := range arbs {
		arbResult := pb.OnChainArbitrage{
			Path: []*pb.OnChainArbitragePathUnit{},
		}

		for _, unit := range arb.Path {
			unitResult := pb.OnChainArbitragePathUnit{
				PoolAddress:     unit.PoolAddress,
				TokenInAddress:  unit.TokenInAddress,
				TokenOutAddress: unit.TokenOutAddress,
				AmountIn:        unit.AmountIn.String(),
				AmountOut:       unit.AmountOut.String(),
			}

			arbResult.Path = append(arbResult.Path, &unitResult)
		}

		result.Arbitrages = append(result.Arbitrages, &arbResult)
	}

	return &result, nil
}
