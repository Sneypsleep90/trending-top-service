package stoplist

import (
	"context"
	"reflect"
	"testing"
)

func TestStopList_AddRemoveContains(t *testing.T) {
	ctx := context.Background()
	stopList := NewMemoryStopList()

	contains, err := stopList.Contains(ctx, "Кроссовки Найк")
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if contains {
		t.Fatal("expected query to be absent")
	}

	if err := stopList.Add(ctx, " Кроссовки Найк "); err != nil {
		t.Fatalf("Add: %v", err)
	}

	contains, err = stopList.Contains(ctx, "кроссовки найк")
	if err != nil {
		t.Fatalf("Contains after Add: %v", err)
	}
	if !contains {
		t.Fatal("expected query to be present")
	}

	if err := stopList.Remove(ctx, "кроссовки найк"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	contains, err = stopList.Contains(ctx, "кроссовки найк")
	if err != nil {
		t.Fatalf("Contains after Remove: %v", err)
	}
	if contains {
		t.Fatal("expected query to be absent after remove")
	}
}

func TestStopList_List(t *testing.T) {
	ctx := context.Background()
	stopList := NewMemoryStopList()

	if err := stopList.Add(ctx, "айфон 15"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := stopList.Add(ctx, "кроссовки найк"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	items, err := stopList.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	want := []string{"айфон 15", "кроссовки найк"}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("expected %#v, got %#v", want, items)
	}
}
