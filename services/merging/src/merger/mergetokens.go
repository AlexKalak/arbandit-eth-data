package merger

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alexkalak/go_market_analyze/common/models"
)

//go:embed mergeassets/tokens.uniswap.org.json
var uniswapTokens string

//go:embed mergeassets/pancakeswap.tokens.json
var pancakeswapTokens string

type tokenFromUniswapOrg struct {
	Name     string `json:"name"`
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
	ChainID  int    `json:"chainId"`
	LogoURI  string `json:"logoURI"`
}

func getTokensByChainID(chainID uint) ([]tokenFromUniswapOrg, error) {
	tokens := []tokenFromUniswapOrg{}
	if chainID == 56 {
		tokensStruct := struct {
			Tokens []tokenFromUniswapOrg `json:"tokens"`
		}{}
		err := json.NewDecoder(strings.NewReader(pancakeswapTokens)).Decode(&tokensStruct)
		if err != nil {
			return nil, err
		}

		tokens = tokensStruct.Tokens
	} else if chainID == 1 {
		err := json.NewDecoder(strings.NewReader(pancakeswapTokens)).Decode(&tokens)
		if err != nil {
			return nil, err
		}
	}

	return tokens, nil
}

func (m *merger) MergeTokens(chainID uint) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	tokensRaw, err := getTokensByChainID(chainID)
	if err != nil {
		return err
	}

	tokens := make([]*models.Token, 0, len(tokensRaw))
	for _, rawToken := range tokensRaw {
		if rawToken.ChainID != int(chainID) {
			continue
		}
		tokens = append(tokens, &models.Token{
			ChainID:  chainID,
			Address:  rawToken.Address,
			Name:     rawToken.Name,
			Symbol:   rawToken.Symbol,
			Decimals: rawToken.Decimals,
			LogoURI:  rawToken.LogoURI,
		})
	}

	query := psql.Insert(models.TOKENS_TABLE).Columns(
		models.TOKEN_ADDRESS,
		models.TOKEN_CHAINID,
		models.TOKEN_NAME,
		models.TOKEN_SYMBOL,
		models.TOKEN_LOGOURI,
		models.TOKEN_DECIMALS,
		models.TOKEN_USD_PRICE,
	)

	tokensMap := map[string]any{}

	fmt.Println(len(tokens))
	for i, token := range tokens {
		if _, ok := tokensMap[token.Address]; ok {
			fmt.Println("found duplicate")
			continue
		}
		tokensMap[token.Address] = new(any)
		token.Address = strings.ToLower(token.Address)

		query = query.Values(
			token.Address,
			token.ChainID,
			token.Name,
			token.Symbol,
			token.LogoURI,
			token.Decimals,
			//defi price shoudlnt be null
			0,
		)
		if (i+1)%2000 == 0 {
			resp, err := query.RunWith(db).Exec()
			if err != nil {
				return err
			}

			rowsAff, err := resp.RowsAffected()
			if err != nil {
				return err
			}
			fmt.Println("Tokens inserted", rowsAff)

			query = psql.Insert(models.TOKENS_TABLE).Columns(
				models.TOKEN_ADDRESS,
				models.TOKEN_CHAINID,
				models.TOKEN_NAME,
				models.TOKEN_SYMBOL,
				models.TOKEN_LOGOURI,
				models.TOKEN_DECIMALS,
				models.TOKEN_USD_PRICE,
			)

		}
	}

	resp, err := query.RunWith(db).Exec()
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	rowsAff, err := resp.RowsAffected()
	if err != nil {
		return err
	}
	fmt.Println("Tokens inserted", rowsAff)

	return nil
}
