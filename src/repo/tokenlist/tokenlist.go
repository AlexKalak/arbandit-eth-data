package tokenlist

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/alexkalak/go_market_analyze/src/errors/tokenerrors"
	"github.com/alexkalak/go_market_analyze/src/helpers"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

var tokenSymbol2Address = "token_symbol_2_address"
var tokenAddress2Data = "token_address_2_data"
var pathToTokensFile = "./src/assets/tokenlist/tokens.bolt"

type TokenList struct {
	boltDB *bolt.DB
}

type UniswapResp struct {
	Tokens []TokenData `json:"tokens"`
}

type BridgeInfo struct {
	TokenAddress string `json:"tokenAddress"`
}

type TokenData struct {
	Name       string `json:"name"`
	Symbol     string `json:"symbol"`
	Address    string `json:"address"`
	ChainID    uint    `json:"chainId"`
	Decimals   int    `json:"decimals"`
	LogoURI    string `json:"logoURI"`
	Extensions struct {
		BridgeInfo map[int]BridgeInfo `json:"bridgeInfo"`
	} `json:"extensions"`
}

var singleton *TokenList

func New() (*TokenList, error) {
	if singleton != nil {
		return singleton, nil
	}

	boltDB, err := bolt.Open(pathToTokensFile, 0600, nil)
	if err != nil {
		return nil, err
	}

	singleton = &TokenList{
		boltDB,
	}

	return singleton, nil
}

func (t *TokenList) UpdateAllTokens() error {
	err := t.EraseAllTokenData()
	if err != nil {
		return err
	}

	tokenData, err := singleton.RequestTokens("https://tokens.pancakeswap.finance/pancakeswap-extended.json")
	if err != nil {
		fmt.Println("2 Fetching tokens error: ", err)
	}
	t.UpdateAllTokensInDB(tokenData.Tokens)

	tokenData, err = singleton.RequestTokens("https://tokens.uniswap.org/")
	if err != nil {
		fmt.Println("1 Fetching tokens error: ", err)
	}
	t.UpdateAllTokensInDB(tokenData.Tokens)

	return nil
}

func (t *TokenList) GetAmountOfTokens() (int, int, error) {
	amountSymbols := 0
	amountAddresses := 0

	err := t.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tokenSymbol2Address))
		if b == nil {
			return bolterrors.ErrBucketNotFound
		}
		amountSymbols = b.Stats().KeyN

		b = tx.Bucket([]byte(tokenAddress2Data))
		if b == nil {
			return bolterrors.ErrBucketNotFound
		}
		amountAddresses = b.Stats().KeyN

		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	return amountSymbols, amountAddresses, nil
}

func (t *TokenList) RequestTokens(url string) (*UniswapResp, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, tokenerrors.ErrUnableToFetchTokens
	}
	defer resp.Body.Close()

	respJSON := UniswapResp{}
	err = json.NewDecoder(resp.Body).Decode(&respJSON)
	if err != nil {
		return nil, tokenerrors.ErrUnableToParseTokenResp
	}

	return &respJSON, err
}

func (t *TokenList) EraseAllTokenData() error {
	err := t.boltDB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(tokenSymbol2Address))
		if err != nil {
			if err != bolterrors.ErrBucketNotFound {
				return err
			}
		}
		err = tx.DeleteBucket([]byte(tokenAddress2Data))
		if err != nil {
			if err != bolterrors.ErrBucketNotFound {
				return err
			}
		}

		return nil
	})

	if err != nil {
		fmt.Println("Erasing tokens db data err: ", err)
		return err
	}
	return nil
}

func (t *TokenList) UpdateAllTokensInDB(tokensJSON []TokenData) error {
	err := t.boltDB.Update(func(tx *bolt.Tx) error {

		bSymbol, err := tx.CreateBucketIfNotExists([]byte(tokenSymbol2Address))
		if err != nil {
			return err
		}
		bAddress, err := tx.CreateBucketIfNotExists([]byte(tokenAddress2Data))
		if err != nil {
			return err
		}

	TokenLoop:
		for _, tokenData := range tokensJSON {
			tokenData.Address = strings.ToLower(tokenData.Address)

			for k, v := range tokenData.Extensions.BridgeInfo {
				tokenData.Extensions.BridgeInfo[k] = BridgeInfo{TokenAddress: strings.ToLower(v.TokenAddress)}
			}

			tokenBytes, err := json.Marshal(tokenData)
			if err != nil {
				continue
			}

			existingSymbol := bSymbol.Get([]byte(tokenData.Symbol))
			if existingSymbol == nil {
				tokenAddressArray := []string{tokenData.Address}

				marshaledTokenAddressArray, err := json.Marshal(tokenAddressArray)
				if err != nil {
					fmt.Println("ERROR WHILE NOT EXISTING MARSHALLING: ", helpers.GetJSONString(tokenData))
					continue
				}

				if err := bSymbol.Put([]byte(tokenData.Symbol), marshaledTokenAddressArray); err != nil {
					continue
				}
			} else {
				tokenAddressArray := []string{}

				err := json.Unmarshal(existingSymbol, &tokenAddressArray)
				if err != nil {
					fmt.Println("ERROR WHILE EXISTING UNMARSHALLING: ", helpers.GetJSONString(tokenData))
					continue
				}

				//Check if address already added to token
				for _, existingTokenAddress := range tokenAddressArray {
					if existingTokenAddress == tokenData.Address {
						continue TokenLoop
					}
				}

				tokenAddressArray = append(tokenAddressArray, tokenData.Address)
				marshaledTokenAddressArray, err := json.Marshal(tokenAddressArray)
				if err != nil {
					fmt.Println("ERROR WHILE EXISTING MARSHALLING: ", helpers.GetJSONString(tokenData))
					continue
				}

				if err := bSymbol.Put([]byte(tokenData.Symbol), marshaledTokenAddressArray); err != nil {
					continue
				}
			}

			if err := bAddress.Put([]byte(tokenData.Address), tokenBytes); err != nil {
				continue
			}
		}

		return nil
	})
	if err != nil {
		fmt.Println("unable to create storage: ", err)
		return tokenerrors.ErrUnableToCreateTokenStorage
	}

	return nil
}

func (t *TokenList) GetTokenBySymbol(symbol string) ([]TokenData, error) {
	tokensData := []TokenData{}
	tokenData := TokenData{}

	err := t.boltDB.View(func(tx *bolt.Tx) error {
		//symbol -> address
		b := tx.Bucket([]byte(tokenSymbol2Address))
		if b == nil {
			return errors.New("bucket is empty")
		}

		tokenAddressArray := []string{}

		addressesBytes := b.Get([]byte(symbol))
		if string(addressesBytes) == "" {
			return errors.New("address not found")
		}

		err := json.Unmarshal(addressesBytes, &tokenAddressArray)
		if err != nil {
			return err
		}

		//address -> tokenData
		b = tx.Bucket([]byte(tokenAddress2Data))
		if b == nil {
			return errors.New("bucket is empty")
		}

		for _, tokenAddress := range tokenAddressArray {
			tokenDataBytes := b.Get([]byte(tokenAddress))
			if string(tokenDataBytes) == "" {
				return errors.New("address to tokenData not found")
			}

			//convert []bytes tokenData to struct
			err := json.Unmarshal(tokenDataBytes, &tokenData)
			if err != nil {
				return err
			}

			tokensData = append(tokensData, tokenData)
		}
		return nil
	})

	if err != nil {
		fmt.Println("error while getting token: ", err)
		return nil, err
	}

	return tokensData, err
}

func (t *TokenList) GetTokenByAddress(address string) (TokenData, error) {
	tokenData := TokenData{}
	address = strings.ToLower(address)

	err := t.boltDB.View(func(tx *bolt.Tx) error {
		//address -> tokenData
		b := tx.Bucket([]byte(tokenAddress2Data))
		if b == nil {
			return errors.New("bucket is empty")
		}
		tokenDataBytes := b.Get([]byte(address))
		if string(tokenDataBytes) == "" {
			return errors.New("address to tokenData not found")
		}

		//convert []bytes tokenData to struct
		err := json.Unmarshal(tokenDataBytes, &tokenData)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		fmt.Println("error while getting token: ", err)
		return TokenData{}, err
	}

	return tokenData, err
}

func (t *TokenList) GetAllTokens() ([]TokenData, error) {
	resTokens := []TokenData{}

	err := t.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tokenAddress2Data))
		if b == nil {
			return errors.New("bucket is empty")
		}

		cursor := b.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			tokenData := TokenData{}
			err := json.Unmarshal(v, &tokenData)
			if err != nil {
				return err
			}

			resTokens = append(resTokens, tokenData)
		}

		return nil
	})

	if err != nil {
		fmt.Println("error while getting token: ", err)
		return nil, err
	}

	return resTokens, err

}
