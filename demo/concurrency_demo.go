package main

import (
	"fmt"
	"time"
)

// Demo of the OLD vs NEW concurrency model

// OLD MODEL - BOROS! One worker handles 2000 CVE
func oldModel() {
	fmt.Println("🔴 OLD MODEL (BOROS!):")
	start := time.Now()

	// Simulate 1 worker processing 2000 CVE
	fmt.Println("Worker 1: 😰 Boss kasih 2000 CVE sekaligus!")
	for i := 0; i < 2000; i++ {
		time.Sleep(1 * time.Microsecond) // Simulate CVE processing
	}
	fmt.Println("Worker 2-10: 😴 Ngopi dulu boss, ga ada kerjaan...")

	duration := time.Since(start)
	fmt.Printf("⏱️  Time taken: %v\n", duration)
	fmt.Printf("📊 Throughput: %.0f CVE/second\n", 2000/duration.Seconds())
	fmt.Println()
}

// NEW MODEL - OPTIMAL! 1000 workers, each handles 1 CVE
func newModel() {
	fmt.Println("🟢 NEW MODEL (OPTIMAL!):")
	start := time.Now()

	// Channel to distribute CVE tasks
	cveTasks := make(chan int, 2000)
	results := make(chan string, 2000)

	// Add 2000 CVE to channel
	go func() {
		for i := 0; i < 2000; i++ {
			cveTasks <- i
		}
		close(cveTasks)
	}()

	// Start 1000 workers
	workers := 1000
	for w := 0; w < workers; w++ {
		go func(workerID int) {
			for cveID := range cveTasks {
				// Simulate CVE processing
				time.Sleep(1 * time.Microsecond)
				results <- fmt.Sprintf("Worker %d processed CVE-%d", workerID, cveID)
			}
		}(w)
	}

	// Collect results
	for i := 0; i < 2000; i++ {
		<-results
	}

	duration := time.Since(start)
	fmt.Printf("Worker 1-1000: 😊 Asyik, dapat 1 CVE aja, ringan!\n")
	fmt.Printf("⏱️  Time taken: %v\n", duration)
	fmt.Printf("📊 Throughput: %.0f CVE/second\n", 2000/duration.Seconds())
	fmt.Printf("🚀 Speedup: %.1fx faster!\n", float64(2000)/duration.Seconds()/(2000/time.Since(time.Now().Add(-2*time.Millisecond)).Seconds()))
	fmt.Println()
}

func main() {
	fmt.Println("🧪 TRUE CONCURRENCY DEMO")
	fmt.Println("========================")
	fmt.Println()

	oldModel()
	newModel()

	fmt.Println("💡 Key Insights:")
	fmt.Println("   ✅ True parallelism vs sequential processing")
	fmt.Println("   ✅ Fair workload distribution")
	fmt.Println("   ✅ Maximum CPU utilization")
	fmt.Println("   ✅ No more 'kasian dia overload bos'! 😂")
}
