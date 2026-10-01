package main

import "fmt"

func printEvent(title string, details ...string) {
	fmt.Printf("\n***** %s *****\n", title)
	for _, detail := range details {
		fmt.Printf("* %s\n", detail)
	}
	fmt.Println("*****")
}
