package main

import (
	"context"
	"log"
	"math"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

type aggregate struct {
	high      float64
	low       float64
	open      float64
	close     float64
	startTime time.Time
	endTime   time.Time
	ticker    string
}

func main() {

	ticker := "X:BTC-USD"
	c := polygon.New("api-key")

	params := models.ListTradesParams{Ticker: ticker}.
		WithTimestamp(models.EQ, models.Nanos(time.Date(2021, 9, 03, 0, 0, 0, 0, time.UTC))).
		WithLimit(5000).
		WithOrder(models.Asc)
	iter := c.ListTrades(context.Background(), params)

	var trades []models.Trade

	for iter.Next() {
		trades = append(trades, iter.Item())

	}

	if iter.Err() != nil {
		log.Fatal(iter.Err())
	}

	currentEndTS := time.Date(2021, 9, 03, 0, 0, 0, 0, time.UTC).Add(30 * time.Second)
	tradesToAggregate := make([]models.Trade, 0)

	for _, trade := range trades {

		if time.Time(trade.ParticipantTimestamp).UnixNano() < currentEndTS.UnixNano() {
			tradesToAggregate = append(tradesToAggregate, trade)
		} else { // calculate aggregate when 30s reached

			if len(tradesToAggregate) == 0 {
				continue // skip aggregation in 30s period without trades
			}
			agg := aggregate{
				open:      tradesToAggregate[0].Price,
				close:     tradesToAggregate[len(tradesToAggregate)-1].Price,
				low:       math.MaxFloat64,
				high:      0.0,
				endTime:   currentEndTS,
				startTime: currentEndTS.Add(-30 * time.Second),
				ticker:    ticker,
			}

			for _, trade := range tradesToAggregate {
				if trade.Price > agg.high {
					agg.high = trade.Price
				}

				if trade.Price < agg.low {
					agg.low = trade.Price
				}

			}
			log.Printf("aggregate: %#v\n", agg)
			currentEndTS = currentEndTS.Add(30 * time.Second)
			tradesToAggregate = make([]models.Trade, 0)

		}
	}

}
