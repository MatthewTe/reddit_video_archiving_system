package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func loadEnvVariablesFromFile(filePath string) error {

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		envFileSplit := strings.Split(scanner.Text(), "=")
		//fmt.Println(envFileSplit)
		if len(envFileSplit) == 2 {
			fmt.Printf("Loading environment variable: %s", envFileSplit[0])
			os.Setenv(envFileSplit[0], envFileSplit[1])
		}

	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
