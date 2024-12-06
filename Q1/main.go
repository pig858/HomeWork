package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	students  = []string{"A", "B", "C", "D", "E"}
	operators = []string{"+", "-", "*", "/"}
)

func main() {
	fmt.Println("Teacher: Guys, are you ready?")

	for {
		time.Sleep(3 * time.Second)

		a := rand.Intn(101)
		b := rand.Intn(101)
		operator := operators[rand.Intn(len(operators))]
		question := fmt.Sprintf("%d %s %d", a, operator, b)
		answer := calcAnswer(a, b, operator)
		fmt.Printf("Teacher: %s = ?\n", question)

		var wg sync.WaitGroup
		answerCh := make(chan string, 1)
		answered := false
		winner := ""

		for _, student := range students {
			wg.Add(1)
			go studentBehavior(student, question, answer, answerCh, &wg, &answered, &winner)
		}

		correctStudent := <-answerCh
		fmt.Printf("Teacher: %s, you are right!\n", correctStudent)

		wg.Wait()
		return
	}
}

func calcAnswer(a int, b int, operator string) float64 {
	switch operator {
	case "+":
		return float64(a + b)
	case "-":
		return float64(a - b)
	case "*":
		return float64(a * b)
	case "/":
		if b != 0 {
			return float64(a) / float64(b)
		}

		return 0
	default:
		return 0
	}
}

func studentBehavior(student string, question string, answer float64, answerCh chan string, wg *sync.WaitGroup, answered *bool, winner *string) {
	defer wg.Done()

	time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)

	if *answered {
		fmt.Printf("Student %s: %s, you win!\n", student, *winner)
		return
	}

	answerCh <- student
	*answered = true
	*winner = student
	fmt.Printf("Student %s: %s = %.2f!\n", student, question, answer)
}
