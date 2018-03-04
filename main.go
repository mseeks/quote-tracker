package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/shopspring/decimal"
	resty "gopkg.in/resty.v1"
)

var (
	apiEndpoint   string
	broker        string
	producerTopic string
)

// The formatter for passing messages into Kafka
type message struct {
	Quote string `json:"quote"`
	At    string `json:"at"`
}

type quoteQuery struct {
	Results []struct {
		LastExtendedHoursTradePrice string `json:"last_extended_hours_trade_price"`
		LastTradePrice              string `json:"last_trade_price"`
		Symbol                      string `json:"symbol"`
	} `json:"results"`
}

// Macro function to run the tracking process
func trackQuotes(equityWatchlist string) error {
	resp, err := resty.R().
		SetQueryParams(map[string]string{
			"symbols": equityWatchlist,
		}).
		SetHeader("Accept", "application/json").
		Get(apiEndpoint)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v, %v", resp.Status(), string(resp.Body()))
	}

	query := quoteQuery{}

	err = json.Unmarshal(resp.Body(), &query)
	if err != nil {
		return err
	}

	kafkaProducer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		return err
	}
	defer kafkaProducer.Close()

	for _, result := range query.Results {
		symbol := result.Symbol
		lastTradePrice := result.LastTradePrice
		lastExtendedHoursTradePrice := result.LastExtendedHoursTradePrice
		quote := lastTradePrice

		if len(lastExtendedHoursTradePrice) > 0 {
			quote = lastExtendedHoursTradePrice
		}

		quoteDecimal, err := decimal.NewFromString(quote)
		if err != nil {
			return err
		}

		quoteMessage := message{
			Quote: quoteDecimal.Round(2).String(),
			At:    time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
		}

		jsonMessage, err := json.Marshal(quoteMessage)
		if err != nil {
			return err
		}

		message := &sarama.ProducerMessage{Topic: producerTopic, Value: sarama.StringEncoder(jsonMessage), Key: sarama.StringEncoder(symbol)}
		_, _, err = kafkaProducer.SendMessage(message)
		if err != nil {
			return err
		}
	}

	return nil
}

// Entrypoint for the program
func main() {
	apiEndpoint = "https://api.robinhood.com/quotes/"
	broker = os.Getenv("KAFKA_ENDPOINT")
	equityWatchlist := os.Getenv("EQUITY_WATCHLIST")
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	for {
		time.Sleep(10 * time.Second)
		err := trackQuotes(equityWatchlist)
		if err != nil {
			fmt.Println(err)
		}
	}
}
