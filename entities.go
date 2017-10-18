package main

import (
	"fmt"
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/trees/btree"
	"github.com/emirpasic/gods/utils"
	"strconv"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var lockUser = sync.RWMutex{}
var lockLocation = sync.RWMutex{}
var lockVisit = sync.RWMutex{}

var lockUserName = sync.RWMutex{}
var lockCountry = sync.RWMutex{}
var lockCity = sync.RWMutex{}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const COMMA = ','
const FBRACET_OPEN = '{'
const FBRACET_CLOSE = '}'

var EMPTY_BODY = []byte("{}")

const QUOTE = "\""

var QUOTE_COMMA = []byte("\",")

const STRING_NULL = "ul" // double quotas are trimmed everywhere... feature!

var ID = []byte("\"id\":")

var USER_FIRST_NAME = []byte("\"first_name\":\"")
var USER_LAST_NAME = []byte("\"last_name\":\"")
var USER_GENDER_NAME = []byte("\"gender\":\"")
var USER_EMAIL_NAME = []byte("\"email\":\"")
var USER_BIRTH_DATE_NAME = []byte("\"birth_date\": ")

var LOC_PLACE = []byte("\"place\":\"")
var LOC_COUNTRY = []byte("\"country\":\"")
var LOC_CITY = []byte("\"city\":\"")
var LOC_DISTANCE = []byte("\"distance\":")

var VIS_LOCATION = []byte("\"location\":")
var VIS_USER = []byte("\"user\": ")
var VIS_VISITED_AT = []byte("\"visited_at\":")
var VIS_MARK = []byte("\"mark\":")

const MIN_TIMESTAMP = -2147483648

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// var VisitCalcIndexChannel chan IEntity
// var VisitPostUpdateChannel chan IEntity

// var IndexChannel chan IEntity

type IEntity interface {
	CopyFromEntity(from IEntity)
	FillProperty(s []byte) bool
	PostUpdate(isNew bool) bool
	Print()
	//GetRaw() []byte
	//SetRaw(s []byte)

	Validate() bool
	ToJson([]byte) []byte

	CalcIndexes() bool
}

func NewEntity(jsonType int, rawJson []byte) (IEntity, bool) {
	var e IEntity

	switch jsonType {
	case USER:
		e = &User{}
	case LOCATION:
		e = &Location{}
	case VISIT:
		e = &Visit{}
	}
	res := ParseProperties(e, rawJson)
	if res {
		switch jsonType {
		case USER:
			if LockFindUser(e.(*User).Id, false) != nil {
				return nil, false
			}
		case LOCATION:
			if LockFindLocation(e.(*Location).Id, false) != nil {
				return nil, false
			}
		case VISIT:
			if LockFindVisit(e.(*Visit).Id, false) != nil {
				return nil, false
			}
		}

		res = e.PostUpdate(true)

		// CalcIndexes
		if res {
			switch jsonType {
			// case USER:
			case LOCATION:
				if STAGE == 0 {
					/*go*/ e.CalcIndexes()
				}
			case VISIT:
				// POST NEW ENTITY
				if STAGE == 1 {
					/*go*/ e.CalcIndexes()
				}
				// fmt.Println("->VisitPostUpdateChannel")
				// VisitCalcIndexChannel <- e
			}
		}
	} else {
		if STAGE == 0 {
			fmt.Println("CORRUPTED DATA:")
			e.Print()
			fmt.Println(string(rawJson))
			//panic("!!!")
		}
		return nil, false
	}

	return e, res
}

// TODO: updating may fail entity if it contains invalid data
func UpdateEntity(jsonType int, e IEntity, rawJson []byte) bool {
	var newEntity IEntity

	switch jsonType {
	case USER:
		newEntity = &User{}
	case LOCATION:
		newEntity = &Location{}
	case VISIT:
		newEntity = &Visit{}
	}
	res := ParseProperties(newEntity, rawJson)

	if res /*&& newEntity.Validate()*/ {
		if jsonType == USER {
			u := e.(*User)

			oldGender := u.Gender

			e.CopyFromEntity(newEntity)
			res = e.PostUpdate(false)

			if oldGender != u.Gender {
				/*go*/ RecalculateUserGender(u, oldGender)
			}
		} else if jsonType == VISIT {
			v := e.(*Visit)

			oldVisitedAtInt := v.VisitedAtInt
			oldLocation := v.Location
			oldUser := v.User

			e.CopyFromEntity(newEntity)
			res = e.PostUpdate(false)

			/*go*/
			RecalculateVisit(v, oldUser, oldLocation, oldVisitedAtInt)
		} else {
			e.CopyFromEntity(newEntity)
			res = e.PostUpdate(false)
		}
	}

	return res
}

func ParseProperties(e IEntity, rawBytes []byte) bool {
	begin := 0

	for i, b := range rawBytes {
		if b == ',' {
			if !e.FillProperty(rawBytes[begin:i]) {
				return false
			}
			begin = i + 1
		}
	}
	return e.FillProperty(rawBytes[begin:])

	//var unicoded []byte
	//unicoded = AppendUnicode(unicoded, rawBytes)
	//e.SetRaw(unicoded)

	//for _, json := range spl {
	//	e.FillProperty(json)
	//}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//var UserIdHash *hashmap.Map = hashmap.New()
//var LocationIdHash *hashmap.Map = hashmap.New()
//var VisitIdHash *hashmap.Map = hashmap.New()

var UserIdHash map[int64]*User = make(map[int64]*User)
var LocationIdHash map[int64]*Location = make(map[int64]*Location)
var VisitIdHash map[int64]*Visit = make(map[int64]*Visit)

//var VisitIdHash *btree.Tree = btree.NewWith(16, utils.int64Comparator)

var CountryHash map[string]*string = make(map[string]*string)
var NameHash map[string]*string = make(map[string]*string)
var CityHash map[string]*string = make(map[string]*string)

func LockFindUser(id int64, need_panic bool) *User {
	lockUser.Lock()
	tmp := FindUser(id, need_panic)
	lockUser.Unlock()
	return tmp
}

func FindUser(id int64, need_panic bool) *User {
	tmp := UserIdHash[id]
	if need_panic {
		if tmp == nil {
			fmt.Println("User not found! ", strconv.FormatInt(id, 10))
		}
	}
	return tmp
}

func LockFindLocation(id int64, need_panic bool) *Location {
	lockLocation.Lock()
	tmp := FindLocation(id, need_panic)
	lockLocation.Unlock()
	return tmp
}

func FindLocation(id int64, need_panic bool) *Location {
	tmp := LocationIdHash[id]
	if need_panic {
		if tmp == nil {
			fmt.Println("Location not found!", strconv.FormatInt(id, 10))
		}
	}
	return tmp
}

func LockFindVisit(id int64, need_panic bool) *Visit {
	lockVisit.Lock()
	tmp := FindVisit(id, need_panic)
	lockVisit.Unlock()
	return tmp
}

func FindVisit(id int64, need_panic bool) *Visit {
	tmp := VisitIdHash[id]
	if need_panic {
		if tmp == nil {
			fmt.Println("Visit not found!", strconv.FormatInt(id, 10))
		}
	}
	return tmp
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type User struct {
	//Raw 		[]byte
	Id        int64
	FirstName *string //[]byte
	LastName  string  //[]byte
	Email     string  //[]byte
	Gender    byte    //[]byte
	//BirthDate 	string

	BirthDateInt int64

	VisitsByDate *btree.Tree // CACHE
	VisitsCount  int

	AgeInt int

	IEntity
}

func (e *User) FillProperty(s []byte) bool {
	//fmt.Println("fill:", string(s))
	var ok bool

	switch s[2] {
	case 'i':
		if e.Id == 0 {
			// e.Id, _ = strconv.ParseUint(string(s[7:]), 10, 64)
			e.Id, ok = strToInt64(s[7:])
			if !ok {
				return false
			}
		}
	case 'f':
		//e.FirstName = string(s[16:len(s) - 1])
		firstName := string(s[16 : len(s)-1])
		if firstName == STRING_NULL {
			return false
		}

		lockUserName.Lock()
		hashed := NameHash[firstName]
		if hashed != nil {
			e.FirstName = hashed
		} else {
			NameHash[firstName] = &firstName
			e.FirstName = &firstName
		}
		lockUserName.Unlock()
	case 'l':
		e.LastName = string(s[15 : len(s)-1])
		if e.LastName == STRING_NULL {
			return false
		}
	case 'b':
		//e.BirthDate = string(s[15:])
		// var err error
		// e.BirthDateInt, err = strconv.ParseInt(/*e.BirthDate*/string(s[15:]), 10, 64)
		e.BirthDateInt, ok = strToInt64(s[15:])
		if !ok {
			return false
		}
	case 'g':
		e.Gender = s[12] //string(s[11:14])
		switch e.Gender {
		case 'm', 'f':
		default:
			return false
		}
	case 'e':
		e.Email = string(s[11 : len(s)-1])
		if e.Email == STRING_NULL {
			return false
		}
	}
	return true
}

//func (e *User) GetRaw() []byte {
//	// return e.Raw
//	return e.ToJson()
//}
//
//func (e *User) SetRaw(s []byte) {
//	e.Raw = s
//}

func (e *User) Validate() bool {
	if CHECK_LOAD_VALIDITY {
		if /*(e.Id == 0) ||*/ (e.FirstName == nil) || (e.LastName == "") || (e.BirthDateInt < MIN_TIMESTAMP || e.BirthDateInt > CURRENT_TIME.Unix()) ||
			(e.Gender == 0) || (e.Email == "") {
			//panic("CORRUPTED DATA")
			//fmt.Println("CORRUPTED DATA")
			// e.Print()
			return false
		}
	}
	return true
}

func (e *User) PostUpdate(isNew bool) bool {
	if isNew {
		lockUser.Lock()
		UserIdHash[e.Id] = e
		lockUser.Unlock()

		e.VisitsByDate = btree.NewWith(12, utils.Int64Comparator)
	}

	// TODO: use time from options.txt
	// go func() {
	e.AgeInt, _, _, _, _, _ = TimeDiff(time.Unix(e.BirthDateInt, 0), CURRENT_TIME)
	// }()
	return true
}

func (e *User) CalcIndexes() bool {
	return true
}

func RecalculateUserGender(u *User, oldGender byte) {
	it := u.VisitsByDate.Iterator()

	var v *Visit
	var l *Location
	for it.Next() {
		v = it.Value().(*Visit)
		l = v.LocationPtr

		switch oldGender {
		case 'm':
			l.MVisits = removeVisit(l.MVisits, v)
			l.FVisits = append(l.FVisits, v)
		case 'f':
			l.FVisits = removeVisit(l.FVisits, v)
			l.MVisits = append(l.MVisits, v)
		}
	}
}

func (e *User) GetUserVisits(fromDate, toDate int64, country string, toDistance int64) *arraylist.List {
	if USE_TIMETRACK {
		defer TimeTrack(time.Now(), "GetUserVisits")
	}

	found := arraylist.New()

	if e.VisitsCount == 0 {
		return found
	}

	//fmt.Println("fromDate:", fromDate, ", toDate:", toDate, ", country:", country, ", toDistance:", toDistance)

	var v *Visit
	var l *Location

	// TODO: btree doesn't have operations to find nearest value

	it := e.VisitsByDate.Iterator()
	for it.Next() {
		v = it.Value().(*Visit)
		l = v.LocationPtr

		if country != "" && *l.Country != country {
			continue
		}

		//fmt.Println(it.Key(), time.Unix(it.Key().(int64), 0))
		if v.VisitedAtInt > fromDate && v.VisitedAtInt < toDate {
			if l.DistanceInt < toDistance {
				found.Add(v)
			}
		}
	}
	return found
}

func (e *User) ToJson(raw []byte) []byte {
	// var raw []byte = make([]byte, 0, 1024) // TODO: GC? Didn't heard!
	raw = append(raw, FBRACET_OPEN)

	raw = append(raw, USER_FIRST_NAME...)
	raw = AppendUnicode(raw, []byte(*e.FirstName))
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, USER_LAST_NAME...)
	raw = AppendUnicode(raw, []byte(e.LastName))
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, USER_GENDER_NAME...)
	raw = append(raw, e.Gender)
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, USER_EMAIL_NAME...)
	raw = append(raw, e.Email...)
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, USER_BIRTH_DATE_NAME...)
	raw = strconv.AppendInt(raw, e.BirthDateInt, 10)
	raw = append(raw, COMMA)

	raw = append(raw, ID...)
	raw = append(raw, strconv.FormatInt(e.Id, 10)...)

	raw = append(raw, FBRACET_CLOSE)

	return raw
}

func (e *User) CopyFromEntity(from IEntity) {
	f := from.(*User)
	if f.FirstName != nil {
		e.FirstName = f.FirstName
	}
	if f.LastName != "" {
		e.LastName = f.LastName
	}
	if f.BirthDateInt != 0 {
		e.BirthDateInt = f.BirthDateInt
	}
	if f.Gender != 0 {
		e.Gender = f.Gender
	}
	if f.Email != "" {
		e.Email = f.Email
	}
}

func (e *User) Print() {
	fmt.Println("[id]:", strconv.FormatInt(e.Id, 10))
	if e.FirstName != nil {
		fmt.Println("first_name:", string(*e.FirstName))
	} else {
		fmt.Println("first_name: nil")
	}
	fmt.Println("last_name:", string(e.LastName))
	fmt.Println("birth_date:", strconv.FormatInt(e.BirthDateInt, 10))
	fmt.Println("gender:", string(e.Gender))
	fmt.Println("email:", string(e.Email))
	//fmt.Println("RAW:", string(e.Raw))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// HINT: Location modifications is safe!
type Location struct {
	//Raw 		[]byte
	Id      int64
	Place   string  //[]byte
	Country *string //string
	City    *string //[]byte
	//Distance	string

	// gender filtration
	// then filter by date and age
	MVisits []*Visit // CACHE
	FVisits []*Visit // CACHE

	DistanceInt int64

	IEntity
}

func (e *Location) FillProperty(s []byte) bool {
	var ok bool
	switch s[2] {
	case 'i':
		if e.Id == 0 {
			// e.Id, _ = strconv.ParseUint(string(s[7:]), 10, 64)
			e.Id, ok = strToInt64(s[7:])
			if !ok {
				return false
			}
		}
	case 'p':
		e.Place = string(s[11 : len(s)-1])
		if e.Place == STRING_NULL {
			return false
		}
	case 'c':
		if s[3] == 'o' {
			country := string(s[13 : len(s)-1])
			if country == STRING_NULL {
				return false
			}
			lockCountry.Lock()
			hashed := CountryHash[country]
			if hashed != nil {
				e.Country = hashed
			} else {
				CountryHash[country] = &country
				e.Country = &country
			}
			lockCountry.Unlock()
		} else {
			//e.City = string(s[10:len(s) - 1])
			city := string(s[10 : len(s)-1])
			if city == STRING_NULL {
				return false
			}
			lockCity.Lock()
			hashed := CityHash[city]
			if hashed != nil {
				e.City = hashed
			} else {
				CityHash[city] = &city
				e.City = &city
			}
			lockCity.Unlock()
		}
	case 'd':
		// e.DistanceInt, _ = strconv.ParseUint(string(s[13:]), 10, 32)
		e.DistanceInt, ok = strToInt64(s[13:])
		if !ok {
			return false
		}
	}
	return true
}

//func (e *Location) GetRaw() []byte {
//	// return e.Raw
//	return e.ToJson()
//}
//
//func (e *Location) SetRaw(s []byte) {
//	e.Raw = s
//}

func (e *Location) Validate() bool {
	if CHECK_LOAD_VALIDITY {
		if (e.Id == 0) || (e.Place == "") || (e.Country == nil) || (e.City == nil) ||
			(e.DistanceInt <= 0) {
			//panic("CORRUPTED DATA")
			//fmt.Println("CORRUPTED DATA")
			return false
		}
	}
	return true
}

func (e *Location) PostUpdate(isNew bool) bool {
	if isNew {
		lockLocation.Lock()
		LocationIdHash[e.Id] = e
		lockLocation.Unlock()
	}
	return true
}

func (e *Location) CalcIndexes() bool {
	e.MVisits = make([]*Visit, 0, 16)
	e.FVisits = make([]*Visit, 0, 16)
	return true
}

func (e *Location) getMarkSumForGender(visits []*Visit, fromDate, toDate int64, fromAge, toAge int) (sum, count int) {
	sum = 0
	count = 0

	if len(visits) == 0 {
		return
	}

	for _, v := range visits {
		//fmt.Println("Vis, usr: ", v.Id, string(v.UserPtr.Gender), v.UserPtr.AgeInt, v.VisitedAtInt, v.MarkInt)
		//fmt.Println(fromDate, toDate, fromAge, toAge)

		if v.VisitedAtInt > fromDate && v.VisitedAtInt < toDate {
			//if v.UserPtr.AgeInt >= fromAge && v.UserPtr.AgeInt < toAge {
			// FIX: 2
			if v.UserPtr.AgeInt >= fromAge && v.UserPtr.AgeInt < toAge {
				sum += v.MarkInt
				count++
			}
		}
	}
	return
}

func (e *Location) GetLocationAvg(fromDate, toDate int64, fromAge, toAge int, gender byte) float64 {
	if USE_TIMETRACK {
		defer TimeTrack(time.Now(), "GetUserVisits")
	}

	mMark := 0
	fMark := 0
	mCount := 0
	fCount := 0

	switch gender {
	case 'm':
		mMark, mCount = e.getMarkSumForGender(e.MVisits, fromDate, toDate, fromAge, toAge)
	case 'f':
		fMark, fCount = e.getMarkSumForGender(e.FVisits, fromDate, toDate, fromAge, toAge)
	default:
		mMark, mCount = e.getMarkSumForGender(e.MVisits, fromDate, toDate, fromAge, toAge)
		fMark, fCount = e.getMarkSumForGender(e.FVisits, fromDate, toDate, fromAge, toAge)
	}

	if mCount+fCount == 0 {
		return 0.0
	}

	return float64(mMark+fMark) / float64(mCount+fCount)
}

func (e *Location) CopyFromEntity(from IEntity) {
	f := from.(*Location)
	if f.Place != "" {
		e.Place = f.Place
	}
	if f.Country != nil {
		e.Country = f.Country
	}
	if f.City != nil {
		e.City = f.City
	}
	if f.DistanceInt > 0 {
		e.DistanceInt = f.DistanceInt
	}
}

func (e *Location) Print() {
	fmt.Println("[id]:", strconv.FormatInt(e.Id, 10))
	fmt.Println("place:", string(e.Place))
	if e.Country != nil {
		fmt.Println("country:", string(*e.Country))
	} else {
		fmt.Println("country: nil")
	}
	if e.City != nil {
		fmt.Println("city:", string(*e.City))
	} else {
		fmt.Println("city: nil")
	}
	fmt.Println("distance:", strconv.FormatInt(e.DistanceInt, 10))
	//fmt.Println("RAW:", string(e.Raw))
}

func (e *Location) ToJson(raw []byte) []byte {
	// var raw []byte = make([]byte, 0, 1024) // TODO: GC? Didn't heard!
	raw = append(raw, FBRACET_OPEN)

	raw = append(raw, LOC_DISTANCE...)
	raw = append(raw, strconv.FormatInt(e.DistanceInt, 10)...)
	raw = append(raw, COMMA)

	raw = append(raw, LOC_CITY...)
	raw = AppendUnicode(raw, []byte(*e.City))
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, LOC_COUNTRY...)
	raw = AppendUnicode(raw, []byte(*e.Country))
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, LOC_PLACE...)
	raw = AppendUnicode(raw, []byte(e.Place))
	raw = append(raw, QUOTE_COMMA...)

	raw = append(raw, ID...)
	raw = append(raw, strconv.FormatInt(e.Id, 10)...)

	raw = append(raw, FBRACET_CLOSE)

	return raw
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Visit struct {
	//Raw 		[]byte
	Id       int64
	Location int64
	User     int64
	//VisitedAt	string

	VisitedAtInt int64
	MarkInt      int
	LocationPtr  *Location // CACHE
	UserPtr      *User     // CACHE

	IEntity
}

func (e *Visit) FillProperty(s []byte) bool {
	var ok bool
	switch s[2] {
	case 'i':
		if e.Id == 0 {
			// e.Id, _ = strconv.ParseUint(string(s[7:]), 10, 64)
			e.Id, ok = strToInt64(s[7:])
			if !ok {
				return false
			}
		}
	case 'l':
		// e.Location, _ = strconv.ParseUint(string(s[13:]), 10, 64)
		e.Location, ok = strToInt64(s[13:])
		if !ok {
			return false
		}
	case 'u':
		// e.User, _ = strconv.ParseUint(string(s[9:]), 10, 64)
		e.User, ok = strToInt64(s[9:])
		if !ok {
			return false
		}
	case 'v':
		//e.VisitedAt = string(s[15:])
		// var err error
		// e.VisitedAtInt, err = strconv.ParseInt(/*e.VisitedAt*/string(s[15:]), 10, 32)
		e.VisitedAtInt, ok = strToInt64(s[15:])
		if !ok {
			return false
		}
		// if err != nil { return false }
	case 'm':
		e.MarkInt = int(s[9]) - int('0')
		if (e.MarkInt < 0) || (e.MarkInt > 5) {
			return false
		}
	}
	return true
}

//func (e *Visit) GetRaw() []byte {
//	// return e.Raw
//	return e.ToJson()
//}
//
//func (e *Visit) SetRaw(s []byte) {
//	e.Raw = s
//}

func (e *Visit) Validate() bool {
	if CHECK_LOAD_VALIDITY {
		// FIX: 3
		if (e.Id == 0) || (e.Location == 0) || (e.User == 0) || /*( e.VisitedAtInt == 0) ||*/
			(e.MarkInt < 0) || (e.MarkInt > 5) || (e.VisitedAtInt > CURRENT_TIME.Unix()) {
			// panic("CORRUPTED DATA")
			//fmt.Println("CORRUPTED DATA")
			return false
		}
	}
	return true
}

func (e *Visit) PostUpdate(isNew bool) bool {
	if isNew {
		lockVisit.Lock()
		VisitIdHash[e.Id] = e
		lockVisit.Unlock()
	}
	return true
}

func (e *Visit) CalcIndexes() bool {
	lockUser.Lock()
	e.UserPtr = FindUser(e.User, true)
	e.UserPtr.VisitsByDate.Put(e.VisitedAtInt, e /*.Id*/)
	e.UserPtr.VisitsCount++
	lockUser.Unlock()

	// go func() {
	lockLocation.Lock()
	e.LocationPtr = FindLocation(e.Location, true)

	switch e.UserPtr.Gender {
	case 'm':
		e.LocationPtr.MVisits = append(e.LocationPtr.MVisits, e)
	case 'f':
		e.LocationPtr.FVisits = append(e.LocationPtr.FVisits, e)
	}
	lockLocation.Unlock()
	// }()
	return true
}

func removeVisit(s []*Visit, d *Visit) []*Visit {
	for i, v := range s {
		if v == d {
			s[i] = s[len(s)-1]
			return s[:len(s)-1]
		}
	}
	return s // TODO: check this
}

func RecalculateVisit(v *Visit, oldUser, oldLocation int64, oldVisitedAtInt int64) {
	oldUserPtr := v.UserPtr
	oldGender := oldUserPtr.Gender
	oldLocationPtr := v.LocationPtr

	if oldUser != v.User {
		// update new user pointer
		v.UserPtr = LockFindUser(v.User, false)
		if v.UserPtr == nil {
			panic("RecalculateVisit, v.UserPtr = nil")
		}
	}

	if oldLocation != v.Location {
		// update new location pointer
		v.LocationPtr = LockFindLocation(v.Location, false)
		if v.LocationPtr == nil {
			panic("RecalculateVisit, v.LocationPtr = nil")
		}
	}

	if oldVisitedAtInt != v.VisitedAtInt || oldUser != v.User {
		oldUserPtr.VisitsByDate.Remove(oldVisitedAtInt)
		oldUserPtr.VisitsCount--

		v.UserPtr.VisitsByDate.Put(v.VisitedAtInt, v)
		v.UserPtr.VisitsCount++
	}

	if oldLocation != v.Location || oldGender != v.UserPtr.Gender {
		// remove from old location
		switch oldGender {
		case 'm':
			oldLocationPtr.MVisits = removeVisit(oldLocationPtr.MVisits, v)
		case 'f':
			oldLocationPtr.FVisits = removeVisit(oldLocationPtr.FVisits, v)
		}

		switch v.UserPtr.Gender {
		case 'm':
			v.LocationPtr.MVisits = append(v.LocationPtr.MVisits, v)
		case 'f':
			v.LocationPtr.FVisits = append(v.LocationPtr.FVisits, v)
		}
	}
}

func (e *Visit) CopyFromEntity(from IEntity) {
	f := from.(*Visit)
	if f.Location != 0 {
		e.Location = f.Location
	}
	if f.User != 0 {
		e.User = f.User
	}
	if f.VisitedAtInt != 0 {
		e.VisitedAtInt = f.VisitedAtInt
	}
	if f.MarkInt != 0 {
		e.MarkInt = f.MarkInt
	}
}

func (e *Visit) Print() {
	fmt.Println("[id]:", strconv.FormatInt(e.Id, 10))
	fmt.Println("location:", string(e.Location))
	fmt.Println("user:", string(e.User))
	fmt.Println("visited_at:", strconv.FormatInt(e.VisitedAtInt, 10))
	fmt.Println("mark:", strconv.FormatInt(int64(e.MarkInt), 10))
	//fmt.Println("RAW:", string(e.Raw))
}

func (e *Visit) ToJson(raw []byte) []byte {
	// var raw []byte = make([]byte, 0, 1024) // TODO: GC? Didn't heard!
	raw = append(raw, FBRACET_OPEN)

	raw = append(raw, VIS_MARK...)
	raw = append(raw, strconv.FormatInt(int64(e.MarkInt), 10)...)
	raw = append(raw, COMMA)

	raw = append(raw, VIS_VISITED_AT...)
	raw = append(raw, strconv.FormatInt(e.VisitedAtInt, 10)...)
	raw = append(raw, COMMA)

	raw = append(raw, VIS_USER...)
	raw = append(raw, strconv.FormatInt(e.User, 10)...)
	raw = append(raw, COMMA)

	raw = append(raw, ID...)
	raw = append(raw, strconv.FormatInt(e.Id, 10)...)
	raw = append(raw, COMMA)

	raw = append(raw, VIS_LOCATION...)
	raw = append(raw, strconv.FormatInt(e.Location, 10)...)

	raw = append(raw, FBRACET_CLOSE)

	return raw
}
