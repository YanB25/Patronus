#include "btree.h"

pthread_mutex_t print_mtx;

/*
 * class btree
 */
btree::btree()
{
    root = (char *) new page();
    height = 1;
}

void btree::setNewRoot(char *new_root)
{
    this->root = (char *) new_root;
    ++height;
}

char *btree::btree_search(entry_key_t key)
{
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL)
    {
        p = (page *) p->linear_search(key);
    }

    page *t;
    while ((t = (page *) p->linear_search(key)) == p->hdr.sibling_ptr)
    {
        p = t;
        if (!p)
        {
            break;
        }
    }

    return (char *) t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char *right)
{  // need to be string
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL)
    {
        p = (page *) p->linear_search(key);
        assert(p != nullptr);
    }

    if (!p->store(this, NULL, key, right))
    {  // store
        btree_insert(key, right);
    }
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left,
                                  entry_key_t key,
                                  char *right,
                                  uint32_t level)
{
    if (level > ((page *) root)->hdr.level)
        return;

    page *p = (page *) this->root;

    while (p->hdr.level > level)
    {
        p = (page *) p->linear_search(key);
    }

    if (!p->store(this, NULL, key, right))
    {
        btree_insert_internal(left, key, right, level);
    }
}

void btree::btree_delete(entry_key_t key)
{
    page *p = (page *) root;

    while (p->hdr.leftmost_ptr != NULL)
    {
        p = (page *) p->linear_search(key);
    }

    page *t;
    while ((t = (page *) p->linear_search(key)) == p->hdr.sibling_ptr)
    {
        p = t;
        if (!p)
            break;
    }

    if (p)
    {
        if (!p->remove(this, key))
        {
            btree_delete(key);
        }
    }
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min,
                               entry_key_t max,
                               unsigned long *buf)
{
    page *p = (page *) root;

    while (p)
    {
        if (p->hdr.leftmost_ptr != NULL)
        {
            // The current page is internal
            p = (page *) p->linear_search(min);
        }
        else
        {
            // Found a leaf
            p->linear_search_range(min, max, buf);

            break;
        }
    }
}

void btree::printAll()
{
    pthread_mutex_lock(&print_mtx);
    int total_keys = 0;
    page *leftmost = (page *) root;
    printf("root: %x\n", root);
    do
    {
        page *sibling = leftmost;
        while (sibling)
        {
            if (sibling->hdr.level == 0)
            {
                total_keys += sibling->hdr.last_index + 1;
            }
            sibling->print();
            sibling = sibling->hdr.sibling_ptr;
        }
        printf("-----------------------------------------\n");
        leftmost = leftmost->hdr.leftmost_ptr;
    } while (leftmost);

    printf("total number of keys: %d\n", total_keys);
    pthread_mutex_unlock(&print_mtx);
}
