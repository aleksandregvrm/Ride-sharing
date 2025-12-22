package main

import (
	"math/rand/v2"
	pb "ride-sharing/shared/proto/driver"
	"ride-sharing/shared/util"
	"sync"

	"github.com/mmcloughlin/geohash"
)

type Service struct {
	drivers []*DriverInMap
	mu      sync.Mutex
}

type DriverInMap struct {
	Driver *pb.Driver
}

func NewService() *Service {
	return &Service{
		drivers: make([]*DriverInMap, 0),
	}
}

func (s *Service) RegisterDriver(driverId, packageSlug string) (*pb.Driver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	randomIndex := rand.IntN(len(PredefinedRoutes))
	randomRoute := PredefinedRoutes[randomIndex]

	randomPlate := GenerateRandomPlate()
	randomAvatar := util.GetRandomAvatar(randomIndex)

	geohash := geohash.Encode(randomRoute[0][0], randomRoute[0][1])

	driver := &pb.Driver{
		Id:              driverId,
		Name:            "Lando Norris",
		ProfilePicture:  randomAvatar,
		CarPlate:        randomPlate,
		Location:        &pb.Location{Latitude: randomRoute[0][0], Longitude: randomRoute[0][1]},
		PackageSlug:     packageSlug,
		GeoHashPosition: geohash,
	}

	s.drivers = append(s.drivers, &DriverInMap{
		Driver: driver,
	})

	return driver, nil
}
