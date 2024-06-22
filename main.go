package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

const BUFFER_SIZE = 2048 * 2048 // 4MB

type StationData struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

func run(wg *sync.WaitGroup) {
	hashMap := make(map[string]*StationData)

	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Make a combined chunk of data
	readBuffer := make([]byte, BUFFER_SIZE)
	leftoverBuffer := make([]byte, 1024)
	leftoverSize := 0
	// chunks := make([][]byte, 0)
	chunkCounter := 0

	for {
		// chunk := make([]byte, BUFFER_SIZE)
		n, err := file.Read(readBuffer)
		if err == io.EOF {
			break
		}

		// chunk = chunk[:n]

		// copy(chunks, chunk)

		if err != nil {
			panic(err)
		}

		// Find the last '\n' (byte=10)
		m := 0
		for i := n - 1; i >= 0; i-- {
			if readBuffer[i] == 10 {
				m = i
				break
			}
		}

		// Create a new chunk with the leftover size
		data := make([]byte, m+leftoverSize)
		// Copy the leftover data to the new chunk
		copy(data, leftoverBuffer[:leftoverSize])
		// Copy the data from the read buffer to the new chunk
		copy(data[leftoverSize:], readBuffer[:m])
		// Adding the leftover chunk to the next chunk to be processed
		copy(leftoverBuffer, readBuffer[m+1:n])
		// Update the leftover size
		leftoverSize = n - m - 1

		// chunks = append(chunks, data)

		fmt.Println("Processing chunk", chunkCounter, "size", len(data))

		wg.Add(1)
		go func(wg *sync.WaitGroup, hashMap map[string]*StationData, data []byte) {
			parseLine(hashMap, data)
			wg.Done()
		}(wg, hashMap, data)

		chunkCounter++
	}

	printResult(hashMap)
}

func parseFloatFast(bs []byte) float64 {
	var intStartIdx int // is negative?
	if bs[0] == '-' {
		intStartIdx = 1
	}

	v := float64(bs[len(bs)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bs) - 3; i >= intStartIdx; i-- { // integer part
		v += float64(bs[i]-'0') * place
		place *= 10
	}

	if intStartIdx == 1 {
		v *= -1
	}
	return v
}

func parseLine(data map[string]*StationData, chunk []byte) {
	scanner := bufio.NewScanner(bytes.NewReader(chunk))
	for scanner.Scan() {
		line := scanner.Bytes()
		// read line and split it by ";"
		parts := bytes.Split(line, []byte{';'})
		name := string(parts[0])
		tempStr := parts[1]

		// convert temperature to float
		temperature := parseFloatFast(tempStr)

		// update data
		station, ok := data[name]
		// if station is not in the map, add it
		if !ok {
			data[name] = &StationData{name, temperature, temperature, temperature, 1}
		} else {
			// update min, max, sum and count

			// update min and max
			if temperature < station.Min {
				station.Min = temperature
			}
			// update max
			if temperature > station.Max {
				station.Max = temperature
			}
			// update sum
			station.Sum += temperature
			station.Count++
		}
	}
}

func printResult(data map[string]*StationData) {
	result := make(map[string]*StationData, len(data))
	keys := make([]string, 0, len(data))
	for _, v := range data {
		keys = append(keys, v.Name)
		result[v.Name] = v
	}
	sort.Strings(keys)

	print("{")
	for _, k := range keys {
		v := result[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, v.Min, v.Sum/float64(v.Count), v.Max)
	}
	print("}\n")
}

func main() {
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	wg := sync.WaitGroup{}
	started := time.Now()
	run(&wg)
	wg.Wait()

	fmt.Printf("%0.6f", time.Since(started).Seconds())
}
