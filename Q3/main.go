package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/klauspost/reedsolomon"
)

type RAID struct {
	Level        int
	Disks        [][]byte
	dataShards   int
	parityShards int
	enc          reedsolomon.Encoder
}

func main() {
	data := "Hello, World!"

	//raid0
	raid0, err := NewRAID(0, 3, 0)
	if err != nil {
		log.Fatalf("Failed to create RAID0: %v", err)
	}
	raid0.Write(data)
	fmt.Println("Written data to RAID0:", data)
	raid0.Clear(1)
	reconstructedRAID0, err := raid0.Read()
	if err != nil {
		log.Fatalf("Failed to Read RAID0: %v", err)
	}
	fmt.Println("Reconstructed data from RAID0:", reconstructedRAID0)

	//raid1
	raid1, err := NewRAID(1, 2, 0)
	if err != nil {
		log.Fatalf("Failed to create RAID1: %v", err)
	}
	raid1.Write(data)
	fmt.Println("Written data to RAID1:", data)
	raid1.Clear(1)
	reconstructedRAID1, err := raid1.Read()
	if err != nil {
		log.Fatalf("Failed to Read RAID0: %v", err)
	}
	fmt.Println("Reconstructed data from RAID1:", reconstructedRAID1)

	//raid10
	raid10, err := NewRAID(10, 2, 2)
	if err != nil {
		log.Fatalf("Failed to create RAID10: %v", err)
	}
	raid10.Write(data)
	fmt.Println("Written data to RAID10:", data)
	raid10.Clear(1)
	reconstructedRAID10, err := raid10.Read()
	if err != nil {
		log.Fatalf("Failed to Read RAID0: %v", err)
	}
	fmt.Println("Reconstructed data from RAID10:", reconstructedRAID10)

	/*
		fixme raid5跟raid6問題 - shard sizes do not match 待解決
		//raid5
		raid5, err := NewRAID(5, 3, 2)
		if err != nil {
			log.Fatalf("Failed to create RAID5: %v", err)
		}
		raid5.Write(data)
		fmt.Println("Written data to RAID5:", data)
		raid5.Clear(1)
		reconstructedRAID5, err := raid5.Read()
		if err != nil {
			log.Fatalf("Failed to Read RAID5: %v", err)
		}
		fmt.Println("Reconstructed data from RAID5:", reconstructedRAID5)

		//raid6
		raid6, err := NewRAID(6, 4, 2)
		if err != nil {
			log.Fatalf("Failed to create RAID6: %v", err)
		}
		raid6.Write(data)
		fmt.Println("Written data to RAID6:", data)
		raid6.Clear(1)
		reconstructedRAID6, err := raid6.Read()
		if err != nil {
			log.Fatalf("Failed to Read RAID6: %v", err)
		}
		fmt.Println("Reconstructed data from RAID6:", reconstructedRAID6)

	*/
}

func NewRAID(level int, dataShards int, parityShards int) (*RAID, error) {

	if dataShards+parityShards <= 0 {
		return nil, errors.New("dataShards and parityShards must be greater than 0")
	}

	var enc reedsolomon.Encoder
	var err error

	if level == 5 || level == 6 {
		enc, err = reedsolomon.New(dataShards, parityShards)
		if err != nil {
			return nil, err
		}
	}

	disks := make([][]byte, dataShards+parityShards)

	raid := &RAID{
		Level:        level,
		Disks:        disks,
		dataShards:   dataShards,
		parityShards: parityShards,
		enc:          enc,
	}

	return raid, nil

}

func (r *RAID) Write(data string) error {

	if len(r.Disks) == 0 {
		return errors.New("disks are not initialized. ")
	}

	stripeSize := len(data) / r.dataShards
	if len(data)%r.dataShards != 0 {
		stripeSize++
	}

	if r.Level == 0 {
		for i := 0; i < r.dataShards; i++ {
			start := i * stripeSize
			end := start + stripeSize
			if end > len(data) {
				end = len(data)
			}
			r.Disks[i] = append(r.Disks[i], []byte(data[start:end])...)
		}
		return nil
	}

	if r.Level == 1 {

		for i := 0; i < r.dataShards; i++ {
			start := i * stripeSize
			end := start + stripeSize
			if end > len(data) {
				end = len(data)
			}

			r.Disks[i] = append(r.Disks[i], []byte(data[start:end])...)
			if i+r.dataShards < len(r.Disks) {
				r.Disks[i+r.dataShards] = append(r.Disks[i+r.dataShards], r.Disks[i]...)
			}
		}
	}

	if r.Level == 10 {
		for i := 0; i < r.dataShards; i++ {
			start := i * stripeSize
			end := start + stripeSize
			if end > len(data) {
				end = len(data)
			}
			group := i / 2
			r.Disks[group*2] = append(r.Disks[group*2], []byte(data[start:end])...)
			r.Disks[group*2+1] = append(r.Disks[group*2+1], []byte(data[start:end])...)
		}
		return nil
	}

	if r.Level == 5 || r.Level == 6 {
		for i := 0; i < r.dataShards; i++ {
			start := i * stripeSize
			end := start + stripeSize
			if end > len(data) {
				end = len(data)
			}
			r.Disks[i] = append(r.Disks[i], []byte(data[start:end])...)
		}

		for i := r.dataShards; i < len(r.Disks); i++ {
			r.Disks[i] = make([]byte, stripeSize)
		}

		err := r.enc.Encode(r.Disks)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RAID) Clear(index int) error {
	if index < 0 || index >= len(r.Disks) {
		return fmt.Errorf("invalid index: %d\n ", index)
	}

	r.Disks[index] = make([]byte, 0)
	return nil
}

func (r *RAID) Read() (string, error) {
	var res []byte

	if r.Level == 0 {
		for i := 0; i < r.dataShards; i++ {
			res = append(res, r.Disks[i]...)
		}
		return string(res), nil

	}

	if r.Level == 1 {
		for i := 0; i < r.dataShards; i++ {
			if len(r.Disks[i]) == 0 {
				for j := 0; j < r.dataShards; j++ {
					if len(r.Disks[j]) > 0 && i != j {
						r.Disks[i] = append([]byte(nil), r.Disks[j]...)
						break
					}
				}
			}
		}
		return string(res), nil
	}

	if r.Level == 10 {
		for group := 0; group < r.dataShards/2; group++ {
			for disk := 0; disk < 2; disk++ {
				i := group*2 + disk
				if len(r.Disks[i]) == 0 {
					otherDisk := group*2 + (1 - disk)
					r.Disks[i] = append([]byte(nil), r.Disks[otherDisk]...)
				}
				res = append(res, r.Disks[i]...)
			}
		}
		return string(res), nil
	}

	if r.Level == 5 || r.Level == 6 {
		for i := 0; i < r.dataShards; i++ {
			res = append(res, r.Disks[i]...)
		}
		err := r.enc.Reconstruct(r.Disks)
		if err != nil {
			return "", err
		}
		return string(res), nil
	}

	return string(res), nil
}
