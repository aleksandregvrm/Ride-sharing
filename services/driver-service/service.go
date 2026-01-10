package main

import (
	"fmt"
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

func (s *Service) FindAvailableDrivers(packageType string) []string {
	var matchingDrivers []string

	for _, driver := range s.drivers {
		if driver.Driver.PackageSlug == packageType {
			matchingDrivers = append(matchingDrivers, driver.Driver.Id)
		}
	}

	if len(matchingDrivers) == 0 {
		return []string{}
	}
	return matchingDrivers
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

func (s *Service) UnregisterDriver(driverId string) (*pb.Driver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, driverInMap := range s.drivers {
		if driverInMap.Driver.Id == driverId {
			s.drivers = append(s.drivers[:i], s.drivers[i+1:]...)
			return &pb.Driver{
				Id:              driverInMap.Driver.Id,
				Name:            driverInMap.Driver.Name,
				ProfilePicture:  driverInMap.Driver.ProfilePicture,
				GeoHashPosition: driverInMap.Driver.GeoHashPosition,
				CarPlate:        driverInMap.Driver.CarPlate,
				PackageSlug:     driverInMap.Driver.PackageSlug,
				Location:        driverInMap.Driver.Location,
			}, nil
		}

	}
	return nil, fmt.Errorf("Could not find a driver with an id of: %s", driverId)
}
